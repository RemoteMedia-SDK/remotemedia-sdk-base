//! Host authorization and manifest selection for mapped source Python plugins.
use super::plugin_toml::{PluginPythonMapped, PluginPythonMeta};
use crate::multiprocess::mapped_python::{
    MappedPythonBundle, MappedPythonConfig, MappedPythonFactory, MappedPythonWheels,
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
    /// Select matching wheels and, on Linux, the bundled namespace supervisor.
    /// Linux uses the configured unshare executable or `/usr/bin/unshare`.
    ///
    /// # Safety
    /// The trust and immutability requirements of [`Self::new`] apply to the
    /// complete bundle and the selected system unshare executable as well.
    pub unsafe fn from_bundle(
        directory: &std::path::Path,
        mut environment: PythonEnvConfig,
        trusted_roots: Vec<PathBuf>,
    ) -> Result<Self> {
        let bundle = MappedPythonBundle::load(directory)?;
        if let Some(supervisor) = bundle.supervisor {
            let unshare = environment
                .process_namespace
                .take()
                .map(|namespace| namespace.unshare)
                .unwrap_or_else(|| PathBuf::from("/usr/bin/unshare"));
            environment.process_namespace = Some(remotemedia_py_env::ProcessNamespace {
                supervisor,
                unshare,
            });
        } else if environment.process_namespace.is_some() {
            return Err(Error::ConfigError(
                "Windows bundle cannot use Linux process_namespace".into(),
            ));
        }
        // SAFETY: caller accepts new's contract for the selected bundle.
        unsafe { Self::new(bundle.wheels, environment, trusted_roots) }
    }

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
        if let Some(version) = mapped.output_copy_protocol {
            if version != 1 {
                return Err(Error::ConfigError(format!(
                    "mapped Python output-copy protocol {version} is unsupported; expected 1"
                )));
            }
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
        if let Some(unknown) = mapped
            .buffer_contracts
            .keys()
            .find(|name| !metadata.node_types.contains(name))
        {
            return Err(Error::ConfigError(format!(
                "mapped Python buffer declaration names unregistered node type '{unknown}'"
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
                self.configure_factory(config, mapped)
            })
            .collect()
    }

    fn configure_factory(
        &self,
        config: MappedPythonConfig,
        mapped: &PluginPythonMapped,
    ) -> Result<MappedPythonFactory> {
        let declaration = mapped.buffer_contracts.get(&config.node_type).cloned();
        // SAFETY: factories has checked this exact package against the roots
        // authorized under new's contract, including its runtime artifacts.
        let factory = unsafe { MappedPythonFactory::for_sessions(config) }
            .with_snapshot_inputs(mapped.snapshot_inputs.clone())?
            .with_snapshot_outputs(mapped.snapshot_outputs.clone())?;
        let factory = if let Some(namespace) = &self.environment.process_namespace {
            // SAFETY: the runtime's caller authorizes these matching executables.
            unsafe {
                factory
                    .with_linux_namespace(namespace.supervisor.clone(), namespace.unshare.clone())
            }?
        } else {
            factory
        };
        let factory = if mapped.output_copy_protocol.is_some() {
            factory.with_output_copies()
        } else {
            factory
        };
        let factory = if let Some(declaration) = declaration {
            factory.with_buffer_contract(move |_, protocol| Ok(Some(declaration.bind(protocol))))
        } else {
            factory
        };
        // The declaration is parameter-independent: inspect it before registration or provisioning.
        crate::nodes::StreamingNodeFactory::try_buffer_contract(
            &factory,
            &serde_json::Value::Null,
        )?;
        Ok(factory)
    }
}
