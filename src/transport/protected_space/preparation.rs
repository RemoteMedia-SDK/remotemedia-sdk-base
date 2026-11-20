use super::{
    lower_hex, ProtectedPreparationBackend, ProtectedPreparationCgroup, ProtectedSpacePhase,
    ProtectedSpaceProfile, VerifiedSpaceExecution,
};
use crate::data::RuntimeData;
use crate::loadable::plugin_toml::{PluginLanguage, PluginToml};
use crate::loadable::source_python::{SourcePythonFactory, SourcePythonPlugin};
use crate::transport::{PipelineExecutor, SessionHandle, TransportData};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod checkout_storage;
mod controller;

const NODE_TYPE: &str = "PrepareSpaceNode";
const ENTRY_MODULE: &str = "hf_spaces_runner.nodes";
const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;

/// Paths supplied by worker configuration after manifest verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectedPreparationSettings {
    pub checkout_root: PathBuf,
    pub checkout_bytes: u64,
    pub policy_file: PathBuf,
    pub python_executable: PathBuf,
    pub cgroup_root: PathBuf,
    pub processes: u32,
    pub workload_backend: ProtectedPreparationBackend,
    pub controller_sha256: String,
}

/// The private input and output of a one-shot preparation pipeline.
#[must_use = "the preparation owner must close and join the session"]
pub struct ProtectedSpacePreparation {
    session: SessionHandle,
    controller: Arc<crate::nodes::python_streaming::PythonStreamingNode>,
    request: Option<String>,
    repository: String,
    commit: String,
    checkout_root: PathBuf,
    _staged_controller: tempfile::TempDir,
}

impl VerifiedSpaceExecution {
    /// Start the verified controller with preinstalled Python. The host must
    /// recheck worker policy before resolution and again before node startup.
    pub async fn start_preparation<F, Fut>(
        &self,
        settings: &ProtectedPreparationSettings,
        cgroup: &ProtectedPreparationCgroup,
        require_current: F,
    ) -> Result<ProtectedSpacePreparation>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let approval = self.approval();
        if approval.phase != ProtectedSpacePhase::Prepare
            || approval.profile != ProtectedSpaceProfile::Isolated
            || approval.target != "linux-amd64-cpu"
        {
            return Err(Error::ConfigError(
                "unavailable protected preparation profile".into(),
            ));
        }
        let (root, policy) = settings.validate()?;
        settings.check_checkout_storage()?;
        cgroup.verify_root(&settings.cgroup_root, settings.processes)?;
        require_current().await?;
        self.require_live_preparation()?;

        let source_root = self.controller_path().to_owned();
        let digest = settings.controller_sha256.clone();
        let staged = tokio::task::spawn_blocking(move || controller::stage(&source_root, &digest))
            .await
            .map_err(|_| invalid_controller())??;
        let manifest = std::fs::read_to_string(staged.path().join("plugin.toml"))
            .map_err(|_| invalid_controller())?;
        let plugin_toml = PluginToml::parse(&manifest).map_err(|_| invalid_controller())?;
        if plugin_toml.plugin.language != PluginLanguage::Python {
            return Err(invalid_controller());
        }
        let source = plugin_toml.python.ok_or_else(invalid_controller)?;
        if source.entry_module != ENTRY_MODULE
            || !source.node_types.iter().any(|name| name == NODE_TYPE)
            || !matches!(source.module_root.as_str(), "" | "." | "./")
            || source.mapped.is_some()
        {
            return Err(invalid_controller());
        }
        let (factory, instance) = SourcePythonFactory::new(SourcePythonPlugin {
            node_type: NODE_TYPE.into(),
            entry_module: source.entry_module,
            module_root: staged.path().to_owned(),
            requires: Vec::new(),
            hash: settings.controller_sha256.clone(),
        })
        .with_protected_python(settings.python_executable.clone(), cgroup.membership());

        // No other session can see this controller factory or its control bus.
        let executor = PipelineExecutor::for_protected_spaces()?;
        executor.register_factory(Arc::new(factory)).await;
        let mut manifest = self.manifest().clone();
        let backend = match settings.workload_backend {
            ProtectedPreparationBackend::Cpu => "cpu",
            ProtectedPreparationBackend::Cuda => "cuda",
        };
        let request = json!({"space_id": approval.repository, "revision": approval.commit,
                             "expected_commit": approval.commit, "backend": backend});
        manifest.nodes[0].params["cache"] = json!(root);
        manifest.nodes[0].params["preparation"] = json!({
            "request": request, "policy_path": policy, "policy_revision": approval.policy_revision,
            "isolated_owner": approval.owner
        });

        require_current().await?;
        self.require_live_preparation()?;
        settings.check_checkout_storage()?;
        let session = executor
            .build_loaded_session(Arc::new(manifest), false)
            .await?;
        let Some(controller) = instance.get().cloned() else {
            let mut session = session;
            session.close().await?;
            session.wait().await?;
            return Err(invalid_controller());
        };
        Ok(ProtectedSpacePreparation {
            session,
            controller,
            request: Some(request.to_string()),
            repository: approval.repository.clone(),
            commit: approval.commit.clone(),
            checkout_root: settings.checkout_root.clone(),
            _staged_controller: staged,
        })
    }

    fn require_live_preparation(&self) -> Result<()> {
        self.check_current()
            .map_err(|error| Error::ConfigError(error.to_string()))
    }
}

impl ProtectedPreparationSettings {
    /// Verify that the advertised controller still has its approved bytes.
    pub async fn check_controller(&self, root: &Path) -> Result<()> {
        let root = root.to_owned();
        let expected = self.controller_sha256.clone();
        tokio::task::spawn_blocking(move || controller::verify(&root, &expected))
            .await
            .map_err(|_| invalid_controller())?
    }

    pub fn check_resources(&self) -> Result<()> {
        self.check_checkout_storage()?;
        ProtectedPreparationCgroup::check(&self.cgroup_root, self.processes)
            .map(|_| ())
            .map_err(|error| Error::ConfigError(format!("protected preparation cgroup: {error}")))
    }

    fn check_checkout_storage(&self) -> Result<()> {
        checkout_storage::check(&self.checkout_root, self.checkout_bytes).map_err(|error| {
            Error::ConfigError(format!("protected preparation checkout storage: {error}"))
        })
    }

    /// Advisory capacity check. The reservation must still verify the budget.
    pub async fn has_process_capacity(&self) -> Result<bool> {
        ProtectedPreparationCgroup::has_capacity(self.cgroup_root.clone(), self.processes).await
    }

    /// Returns `None` when other controllers reserve the remaining budget.
    pub async fn try_reserve_cgroup(&self) -> Result<Option<ProtectedPreparationCgroup>> {
        ProtectedPreparationCgroup::reserve(self.cgroup_root.clone(), self.processes).await
    }

    pub async fn reserve_cgroup(&self) -> Result<ProtectedPreparationCgroup> {
        self.try_reserve_cgroup().await?.ok_or_else(|| {
            Error::ConfigError(
                "protected preparation cgroup: protected preparation process capacity is reserved"
                    .into(),
            )
        })
    }

    fn validate(&self) -> Result<(&str, &str)> {
        let invalid = || Error::ConfigError("invalid worker preparation settings".into());
        if !self.checkout_root.is_absolute()
            || !self.policy_file.is_absolute()
            || !self.python_executable.is_absolute()
            || !self.checkout_root.is_dir()
            || !self.policy_file.is_file()
            || !self.python_executable.is_file()
            || !lower_hex(&self.controller_sha256, 64)
        {
            return Err(invalid());
        }
        Ok((
            self.checkout_root.to_str().ok_or_else(invalid)?,
            self.policy_file.to_str().ok_or_else(invalid)?,
        ))
    }
}

impl ProtectedSpacePreparation {
    /// Submit the single approved request and read its bounded descriptor.
    pub async fn resolve(&mut self) -> Result<String> {
        let request = self
            .request
            .take()
            .ok_or_else(|| Error::Execution("protected preparation already submitted".into()))?;
        self.session
            .send_input(TransportData::new(RuntimeData::Text(request)))
            .await?;
        let output = self.session.recv_output().await?;
        let descriptor = match output.map(|output| output.data) {
            Some(RuntimeData::Json(descriptor)) => descriptor.to_string(),
            Some(RuntimeData::Text(descriptor)) => {
                crate::data::split_text_str(&descriptor).1.to_owned()
            }
            _ => return Err(invalid_descriptor()),
        };
        validate_descriptor(
            &descriptor,
            &self.repository,
            &self.commit,
            &self.checkout_root,
        )?;
        Ok(descriptor)
    }

    /// Stop the controller and wait for its worker to exit.
    pub async fn close(mut self) -> Result<()> {
        let close = self.session.close().await;
        let joined = self.session.wait().await;
        let reaped = self.controller.reap_protected_worker().await;
        close.and(joined).and(reaped)
    }
}

fn validate_descriptor(bytes: &str, repository: &str, commit: &str, root: &Path) -> Result<()> {
    if bytes.len() > MAX_DESCRIPTOR_BYTES {
        return Err(invalid_descriptor());
    }
    let descriptor: Value = serde_json::from_str(bytes).map_err(|_| invalid_descriptor())?;
    if descriptor["preparation_error"].as_str() == Some("failed") {
        return Err(Error::Execution(
            "protected preparation controller reported failure".into(),
        ));
    }
    if descriptor["space_id"].as_str() != Some(repository)
        || descriptor["revision"].as_str() != Some(commit)
        || !descriptor["source_tree_sha256"]
            .as_str()
            .is_some_and(|digest| lower_hex(digest, 64))
    {
        return Err(invalid_descriptor());
    }
    let checkout = descriptor["checkout"]
        .as_str()
        .filter(|value| Path::new(value).is_absolute())
        .ok_or_else(invalid_descriptor)?;
    let checkout = std::fs::canonicalize(checkout).map_err(|_| invalid_descriptor())?;
    let root = std::fs::canonicalize(root).map_err(|_| invalid_descriptor())?;
    if checkout == root || !checkout.starts_with(root) || !checkout.is_dir() {
        return Err(invalid_descriptor());
    }
    Ok(())
}

fn invalid_controller() -> Error {
    Error::ConfigError("invalid protected preparation controller".into())
}

fn invalid_descriptor() -> Error {
    Error::Execution("invalid protected preparation descriptor".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::protected_space::tests::{encode, fixture};

    #[test]
    fn preparation_descriptor_is_bound_to_the_approved_source() {
        let root = tempfile::tempdir().unwrap();
        let checkout = root.path().join("checkout");
        std::fs::create_dir(&checkout).unwrap();
        let commit = "a".repeat(40);
        let expected = json!({"space_id":"org/demo", "revision":commit, "checkout":checkout,
                              "source_tree_sha256":"c".repeat(64)});
        let validate = |descriptor: &Value| {
            validate_descriptor(&descriptor.to_string(), "org/demo", &commit, root.path())
        };
        validate(&expected).unwrap();
        let failure = validate(&json!({"preparation_error": "failed"})).unwrap_err();
        assert!(failure.to_string().contains("controller reported failure"));
        let outside = tempfile::tempdir().unwrap();
        let link = root.path().join("host");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        for (field, value) in [
            ("space_id", json!("org/other")),
            ("revision", json!("b".repeat(40))),
            ("source_tree_sha256", json!("unverified")),
            ("checkout", json!("checkout")),
            ("checkout", json!(link)),
            ("checkout", json!(root.path())),
        ] {
            let mut descriptor = expected.clone();
            descriptor[field] = value;
            assert!(validate(&descriptor).is_err(), "accepted altered {field}");
        }
    }

    #[tokio::test]
    #[ignore = "requires a prebuilt Spaces controller, delegated cgroup, bounded checkout mount, and public Hub access"]
    async fn verified_preparation_executes_the_real_source_controller() {
        let plugin = PathBuf::from(std::env::var_os("RM_SPACE_PREPARATION_PLUGIN").unwrap());
        let python = PathBuf::from(std::env::var_os("RM_SPACE_PREPARATION_PYTHON").unwrap());
        let root = tempfile::tempdir().unwrap();
        let checkout_root =
            PathBuf::from(std::env::var_os("RM_SPACE_PREPARATION_CHECKOUT").unwrap());
        let policy_file = root.path().join("policy.json");
        let (mut approval, _, mut manifest) = fixture(ProtectedSpacePhase::Prepare);
        approval.repository = "not-lain/background-removal".into();
        approval.commit = "448a02eff7df7ee3f7ab6026d7882561f895911a".into();
        let policy = format!(
            r#"{{"version":2,"reviewed":{{"{}":{{"{}":{{"cpu":[]}}}}}}}}"#,
            approval.repository, approval.commit
        );
        std::fs::write(&policy_file, &policy).unwrap();
        use sha2::{Digest, Sha256};
        approval.policy_revision = format!("{:x}", Sha256::digest(policy.as_bytes()));
        manifest["plugins"][0]["path"] = json!(plugin);
        let bytes = encode(&mut approval, &manifest);
        let executor = PipelineExecutor::for_protected_spaces().unwrap();
        let execution = executor
            .verify_protected_space(approval, &plugin, &bytes)
            .unwrap();
        let settings = ProtectedPreparationSettings {
            checkout_root,
            checkout_bytes: std::env::var("RM_SPACE_PREPARATION_CHECKOUT_BYTES")
                .unwrap()
                .parse()
                .unwrap(),
            policy_file,
            python_executable: python,
            cgroup_root: PathBuf::from(std::env::var_os("RM_SPACE_PREPARATION_CGROUP").unwrap()),
            processes: 32,
            workload_backend: ProtectedPreparationBackend::Cpu,
            controller_sha256: controller::digest_fixture(&plugin),
        };
        let cgroup = settings.reserve_cgroup().await.unwrap();
        let rechecks = std::cell::Cell::new(0);
        let mut session = execution
            .start_preparation(&settings, &cgroup, || {
                rechecks.set(rechecks.get() + 1);
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert_eq!(rechecks.get(), 2);
        let descriptor =
            tokio::time::timeout(std::time::Duration::from_secs(90), session.resolve()).await;
        session.close().await.unwrap();
        cgroup.close().await.unwrap();
        let descriptor: Value = serde_json::from_str(&descriptor.unwrap().unwrap()).unwrap();
        assert_eq!(descriptor["space_id"], "not-lain/background-removal");
        assert!(executor.control_bus().list().is_empty());
    }
}
