use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use remotemedia_bundle::{
    sha256_digest, DescriptorIdentity, InstallStatus, PreflightReport, ProvisioningPhase,
    RuntimeCapabilities, VariantCandidate,
};

use crate::{
    preflight, ActivationRegistry, ContentStore, DeploymentError, DeploymentInfo,
    DeploymentRevision, ExternalAssetTransport, UploadSession,
};

#[derive(Clone)]
pub struct TokenAuthenticator {
    token_digest: String,
}

impl TokenAuthenticator {
    pub fn new(token: &[u8]) -> Self {
        Self {
            token_digest: sha256_digest(token),
        }
    }

    fn authorize(&self, token: &[u8]) -> Result<(), DeploymentError> {
        let candidate = sha256_digest(token);
        let mut difference = 0_u8;
        for (left, right) in candidate.bytes().zip(self.token_digest.bytes()) {
            difference |= left ^ right;
        }
        if difference == 0 {
            Ok(())
        } else {
            Err(DeploymentError::Unauthenticated)
        }
    }
}

#[derive(Clone)]
pub struct DeploymentService {
    authenticator: TokenAuthenticator,
    capabilities: RuntimeCapabilities,
    content: ContentStore,
    registry: Arc<Mutex<ActivationRegistry>>,
    operations: Arc<Mutex<BTreeMap<String, Operation>>>,
    flights: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    external_asset_transport: Option<Arc<dyn ExternalAssetTransport + Send + Sync>>,
}

#[derive(Clone)]
struct Operation {
    status: InstallStatus,
    cancelled: bool,
}

impl DeploymentService {
    pub fn new(
        authenticator: TokenAuthenticator,
        capabilities: RuntimeCapabilities,
        content: ContentStore,
        registry: ActivationRegistry,
    ) -> Self {
        Self {
            authenticator,
            capabilities,
            content,
            registry: Arc::new(Mutex::new(registry)),
            operations: Arc::new(Mutex::new(BTreeMap::new())),
            flights: Arc::new(Mutex::new(BTreeMap::new())),
            external_asset_transport: None,
        }
    }

    pub fn with_external_asset_transport(
        mut self,
        transport: Arc<dyn ExternalAssetTransport + Send + Sync>,
    ) -> Self {
        self.external_asset_transport = Some(transport);
        self
    }

    pub fn capabilities(&self, token: &[u8]) -> Result<RuntimeCapabilities, DeploymentError> {
        self.authenticator.authorize(token)?;
        Ok(self.capabilities.clone())
    }

    pub fn list_deployments(&self, token: &[u8]) -> Result<Vec<DeploymentInfo>, DeploymentError> {
        self.authenticator.authorize(token)?;
        Ok(self.registry.lock().expect("registry lock poisoned").list())
    }

    pub fn active_manifest(
        &self,
        token: &[u8],
        name: &str,
    ) -> Result<(DeploymentRevision, Vec<u8>), DeploymentError> {
        self.authenticator.authorize(token)?;
        let revision = self
            .registry
            .lock()
            .expect("registry lock poisoned")
            .active(name)
            .cloned()
            .ok_or_else(|| DeploymentError::NotInstalled(name.to_owned()))?;
        let manifest_digest = revision
            .manifest_digest
            .as_ref()
            .ok_or_else(|| DeploymentError::MissingManifestDigest(name.to_owned()))?;
        let manifest = self.content.read(manifest_digest)?;
        Ok((revision.clone(), self.resolve_native_plugin_paths(&revision, manifest)?))
    }

    pub fn preflight(
        &self,
        token: &[u8],
        variants: &[VariantCandidate],
    ) -> Result<PreflightReport, DeploymentError> {
        self.authenticator.authorize(token)?;
        let cached = variants
            .iter()
            .flat_map(|variant| &variant.required_blobs)
            .filter(|blob| self.content.contains(&blob.digest))
            .map(|blob| blob.digest.clone())
            .collect::<BTreeSet<_>>();
        Ok(preflight(&self.capabilities, variants, &cached))
    }

    pub fn missing_blobs(
        &self,
        token: &[u8],
        descriptors: &[DescriptorIdentity],
    ) -> Result<Vec<DescriptorIdentity>, DeploymentError> {
        self.authenticator.authorize(token)?;
        Ok(self.content.missing(descriptors))
    }

    pub fn begin_upload(
        &self,
        token: &[u8],
        descriptor: DescriptorIdentity,
    ) -> Result<UploadSession, DeploymentError> {
        self.authenticator.authorize(token)?;
        self.content.begin_upload(descriptor)
    }

    pub fn status(
        &self,
        token: &[u8],
        operation_id: &str,
    ) -> Result<Option<InstallStatus>, DeploymentError> {
        self.authenticator.authorize(token)?;
        Ok(self
            .operations
            .lock()
            .expect("operation lock poisoned")
            .get(operation_id)
            .map(|operation| operation.status.clone()))
    }

    pub fn cancel(&self, token: &[u8], operation_id: &str) -> Result<(), DeploymentError> {
        self.authenticator.authorize(token)?;
        if let Some(operation) = self
            .operations
            .lock()
            .expect("operation lock poisoned")
            .get_mut(operation_id)
        {
            operation.cancelled = true;
        }
        Ok(())
    }

    pub fn install<F>(
        &self,
        token: &[u8],
        operation_id: &str,
        revision: DeploymentRevision,
        total_bytes: u64,
        mut provision: F,
    ) -> Result<(), DeploymentError>
    where
        F: FnMut(ProvisioningPhase) -> Result<(), String>,
    {
        self.authenticator.authorize(token)?;
        if let Some(missing) = revision
            .content_digests
            .iter()
            .find(|digest| !self.content.contains(digest))
        {
            return Err(DeploymentError::Provisioning(format!(
                "required verified content is missing: {missing}"
            )));
        }
        {
            let mut operations = self.operations.lock().expect("operation lock poisoned");
            if operations.contains_key(operation_id) {
                return Err(DeploymentError::OperationExists(operation_id.to_owned()));
            }
            operations.insert(
                operation_id.to_owned(),
                Operation {
                    status: InstallStatus {
                        operation_id: operation_id.to_owned(),
                        phase: ProvisioningPhase::Resolving,
                        completed_bytes: 0,
                        total_bytes: Some(total_bytes),
                        diagnostic: None,
                    },
                    cancelled: false,
                },
            );
        }

        let flight = {
            let mut flights = self.flights.lock().expect("flight lock poisoned");
            flights
                .entry(revision.bundle_digest.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _flight_guard = flight.lock().expect("content flight lock poisoned");
        if self
            .registry
            .lock()
            .expect("registry lock poisoned")
            .is_installed(&revision.bundle_digest)
        {
            self.set_status(operation_id, ProvisioningPhase::Ready, total_bytes, None);
            return Ok(());
        }
        let phases = [
            ProvisioningPhase::Resolving,
            ProvisioningPhase::Transferring,
            ProvisioningPhase::Verifying,
            ProvisioningPhase::InstallingPython,
            ProvisioningPhase::FetchingAssets,
            ProvisioningPhase::Loading,
            ProvisioningPhase::Warming,
            ProvisioningPhase::SmokeTesting,
        ];
        for phase in phases {
            if self.is_cancelled(operation_id) {
                self.set_status(
                    operation_id,
                    ProvisioningPhase::Cancelled,
                    0,
                    Some("operation cancelled".to_owned()),
                );
                return Err(DeploymentError::Cancelled(operation_id.to_owned()));
            }
            self.set_status(operation_id, phase, 0, None);
            if phase == ProvisioningPhase::FetchingAssets {
                if let Err(error) = self.fetch_external_assets(&revision) {
                    let diagnostic = bounded_diagnostic(&error.to_string());
                    self.set_status(
                        operation_id,
                        ProvisioningPhase::Failed,
                        0,
                        Some(diagnostic.clone()),
                    );
                    return Err(DeploymentError::Provisioning(diagnostic));
                }
            }
            if phase == ProvisioningPhase::Loading {
                if revision.native_runtime.is_some() || !revision.external_assets.is_empty() {
                    let mut closure = revision.native_runtime.clone().unwrap_or(
                        remotemedia_bundle::NativeRuntimeClosure {
                            files: Vec::new(),
                            symlinks: Vec::new(),
                        },
                    );
                    closure.files.extend(revision.external_assets.iter().map(|asset| {
                        remotemedia_bundle::NativeRuntimeFile {
                            digest: asset.digest.clone(),
                            size: asset.size,
                            path: format!("assets/{}", asset.name),
                            mode: remotemedia_bundle::NativeRuntimeFileMode::ReadOnly,
                        }
                    }));
                    if let Err(error) = self
                        .content
                        .materialize_native_runtime(&revision.bundle_digest, &closure)
                    {
                        let diagnostic = bounded_diagnostic(&error.to_string());
                        self.set_status(
                            operation_id,
                            ProvisioningPhase::Failed,
                            0,
                            Some(diagnostic.clone()),
                        );
                        return Err(DeploymentError::Provisioning(diagnostic));
                    }
                }
            }
            if let Err(diagnostic) = provision(phase) {
                let diagnostic = bounded_diagnostic(&diagnostic);
                self.set_status(
                    operation_id,
                    ProvisioningPhase::Failed,
                    0,
                    Some(diagnostic.clone()),
                );
                return Err(DeploymentError::Provisioning(diagnostic));
            }
        }
        self.registry
            .lock()
            .expect("registry lock poisoned")
            .record_installed(revision)?;
        self.set_status(operation_id, ProvisioningPhase::Ready, total_bytes, None);
        Ok(())
    }

    pub fn activate(
        &self,
        token: &[u8],
        name: &str,
        bundle_digest: &str,
    ) -> Result<(), DeploymentError> {
        self.authenticator.authorize(token)?;
        self.registry
            .lock()
            .expect("registry lock poisoned")
            .activate(name, bundle_digest)
    }

    pub fn rollback(&self, token: &[u8], name: &str) -> Result<String, DeploymentError> {
        self.authenticator.authorize(token)?;
        self.registry
            .lock()
            .expect("registry lock poisoned")
            .rollback(name)
    }

    fn is_cancelled(&self, operation_id: &str) -> bool {
        self.operations
            .lock()
            .expect("operation lock poisoned")
            .get(operation_id)
            .is_some_and(|operation| operation.cancelled)
    }

    fn fetch_external_assets(&self, revision: &DeploymentRevision) -> Result<(), DeploymentError> {
        if revision.external_assets.is_empty() {
            return Ok(());
        }
        let transport = self.external_asset_transport.as_ref().ok_or_else(|| {
            DeploymentError::Provisioning(
                "runtime has no external asset transport configured".to_owned(),
            )
        })?;
        for asset in &revision.external_assets {
            self.content
                .fetch_external_asset(asset, transport.as_ref())?;
        }
        Ok(())
    }

    fn resolve_native_plugin_paths(
        &self,
        revision: &DeploymentRevision,
        manifest: Vec<u8>,
    ) -> Result<Vec<u8>, DeploymentError> {
        let Some(closure) = &revision.native_runtime else {
            return Ok(manifest);
        };
        let release = self.content.native_runtime_release(&revision.bundle_digest)?;
        let plugin_paths: Vec<_> = closure
            .files
            .iter()
            .filter(|file| file.path.starts_with("plugins/"))
            .map(|file| release.join(&file.path).to_string_lossy().into_owned())
            .collect();
        if plugin_paths.is_empty() {
            return Ok(manifest);
        }
        let mut value: serde_json::Value = serde_json::from_slice(&manifest)?;
        let plugins = value
            .get_mut("plugins")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| DeploymentError::Provisioning("native runtime has no manifest plugins".to_owned()))?;
        if plugins.len() != plugin_paths.len() {
            return Err(DeploymentError::Provisioning(
                "native runtime plugin count does not match manifest plugins".to_owned(),
            ));
        }
        for (plugin, path) in plugins.iter_mut().zip(plugin_paths) {
            *plugin = serde_json::json!({"path": path});
        }
        Self::resolve_asset_paths(&mut value, &release, &revision.external_assets);
        Ok(remotemedia_bundle::canonical_json(&value)?)
    }


fn resolve_asset_paths(
    value: &mut serde_json::Value,
    release: &std::path::Path,
    assets: &[remotemedia_bundle::AssetDescriptor],
) {
    match value {
        serde_json::Value::String(path) => {
            if let Some(asset) = assets.iter().find(|asset| {
                std::path::Path::new(path)
                    .file_name()
                    .is_some_and(|name| name == std::ffi::OsStr::new(&asset.name))
            }) {
                *path = release.join("assets").join(&asset.name).to_string_lossy().into_owned();
            }
        }
        serde_json::Value::Array(values) => {
            for value in values { Self::resolve_asset_paths(value, release, assets); }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() { Self::resolve_asset_paths(value, release, assets); }
        }
        _ => {}
    }
}
    fn set_status(
        &self,
        operation_id: &str,
        phase: ProvisioningPhase,
        completed_bytes: u64,
        diagnostic: Option<String>,
    ) {
        if let Some(operation) = self
            .operations
            .lock()
            .expect("operation lock poisoned")
            .get_mut(operation_id)
        {
            operation.status.phase = phase;
            operation.status.completed_bytes = completed_bytes;
            operation.status.diagnostic = diagnostic;
        }
    }
}

fn bounded_diagnostic(value: &str) -> String {
    let mut redacted = String::with_capacity(value.len().min(1024));
    let mut word = String::new();
    let mut redact_next = false;
    for character in value.chars() {
        if character.is_whitespace() {
            append_safe_word(&mut redacted, &mut word, &mut redact_next);
            if redacted.len() < 1024 {
                redacted.push(' ');
            }
        } else if character.is_control() {
            append_safe_word(&mut redacted, &mut word, &mut redact_next);
            if redacted.len() < 1024 {
                redacted.push(' ');
            }
        } else {
            word.push(character);
        }
        if redacted.len() >= 1024 {
            break;
        }
    }
    append_safe_word(&mut redacted, &mut word, &mut redact_next);
    redacted.truncate(1024);
    redacted.trim().to_owned()
}

fn append_safe_word(output: &mut String, word: &mut String, redact_next: &mut bool) {
    if word.is_empty() || output.len() >= 1024 {
        word.clear();
        return;
    }
    let lowercase = word.to_ascii_lowercase();
    let sensitive = *redact_next
        || lowercase.contains("authorization:")
        || lowercase.contains("token=")
        || has_url_credentials(&lowercase);
    if sensitive {
        word.clear();
        if output.len() < 1024 {
            output.push_str("[REDACTED]");
        }
        *redact_next = false;
    } else if lowercase == "bearer" {
        word.clear();
        if output.len() < 1024 {
            output.push_str("[REDACTED]");
        }
        *redact_next = true;
    } else {
        output.push_str(word);
        word.clear();
    }
}

fn has_url_credentials(value: &str) -> bool {
    let Some(scheme_end) = value.find("://") else {
        return false;
    };
    let authority = &value[scheme_end + 3..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::{Cursor, Read};
    use std::sync::Arc;

    use remotemedia_bundle::{
        sha256_digest, AcceleratorBackend, AssetDescriptor, AssetStorage, CompatibilityRange,
        CredentialPolicy, DescriptorIdentity,
    };

    use super::*;

    fn service(root: &std::path::Path) -> DeploymentService {
        DeploymentService::new(
            TokenAuthenticator::new(b"secret"),
            RuntimeCapabilities {
                schema_version: remotemedia_bundle::BUNDLE_SCHEMA_VERSION.to_owned(),
                os: "linux".to_owned(),
                architecture: "amd64".to_owned(),
                native_abi: None,
                manifest_schemas: vec!["v1".to_owned()],
                plugin_abi: CompatibilityRange {
                    minimum: "0.4.0".to_owned(),
                    maximum_exclusive: None,
                },
                python: Vec::new(),
                accelerators: vec![AcceleratorBackend::Cpu],
                memory_bytes: 1024,
                available_cache_bytes: 1024,
                media_devices: Vec::new(),
                runtime_features: Vec::new(),
            },
            ContentStore::open(root.join("cas")).unwrap(),
            ActivationRegistry::open(root.join("state")).unwrap(),
        )
    }

    fn revision(name: &str) -> DeploymentRevision {
        DeploymentRevision {
            bundle_digest: name.to_owned(),
            variant_digest: format!("{name}-variant"),
            manifest_digest: None,
            content_digests: BTreeSet::new(),
            external_assets: Vec::new(),
            native_runtime: None,
        }
    }

    #[test]
    fn active_manifest_returns_the_active_revision_from_verified_content() {
        let temp = tempfile::tempdir().unwrap();
        let content = ContentStore::open(temp.path().join("cas")).unwrap();
        let manifest = br#"{\"version\":\"v1\",\"metadata\":{\"name\":\"smoke\"},\"nodes\":[],\"connections\":[]}"#;
        let manifest_digest = sha256_digest(manifest);
        let mut upload = content
            .begin_upload(DescriptorIdentity {
                digest: manifest_digest.clone(),
                size: manifest.len() as u64,
            })
            .unwrap();
        upload.append(0, manifest).unwrap();
        upload.finish().unwrap();
        let service = DeploymentService::new(
            TokenAuthenticator::new(b"secret"),
            service_capabilities(),
            content,
            ActivationRegistry::open(temp.path().join("state")).unwrap(),
        );
        let mut revision = revision("bundle");
        revision.manifest_digest = Some(manifest_digest.clone());
        revision.content_digests.insert(manifest_digest);
        service
            .install(b"secret", "install", revision, manifest.len() as u64, |_| Ok(()))
            .unwrap();
        service.activate(b"secret", "smoke", "bundle").unwrap();

        let (active, bytes) = service.active_manifest(b"secret", "smoke").unwrap();
        assert_eq!(active.bundle_digest, "bundle");
        assert_eq!(bytes, manifest);
    }

    #[test]
    fn native_runtime_rewrites_plugin_paths_to_the_release() {
        let temp = tempfile::tempdir().unwrap();
        let service = service(temp.path());
        let revision = DeploymentRevision {
            bundle_digest: format!("sha256:{}", "d".repeat(64)),
            variant_digest: "variant".to_owned(),
            manifest_digest: None,
            content_digests: BTreeSet::new(),
            external_assets: Vec::new(),
            native_runtime: Some(remotemedia_bundle::NativeRuntimeClosure {
                files: vec![remotemedia_bundle::NativeRuntimeFile {
                    digest: format!("sha256:{}", "e".repeat(64)),
                    size: 1,
                    path: "plugins/libplugin.so".to_owned(),
                    mode: remotemedia_bundle::NativeRuntimeFileMode::Executable,
                }],
                symlinks: Vec::new(),
            }),
        };
        let manifest = br#"{"plugins":[{"path":"./build/libplugin.so"}]}"#.to_vec();
        let resolved = service.resolve_native_plugin_paths(&revision, manifest).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&resolved).unwrap();
        assert_eq!(
            value["plugins"][0]["path"],
            format!(
                "{}/releases/{}/plugins/libplugin.so",
                temp.path().join("cas").display(),
                "d".repeat(64)
            )
        );
    }

    #[test]
    fn authentication_is_required_for_every_operation() {
        let temp = tempfile::tempdir().unwrap();
        let service = service(temp.path());
        assert!(matches!(
            service.capabilities(b"wrong"),
            Err(DeploymentError::Unauthenticated)
        ));
        assert!(service.capabilities(b"secret").is_ok());
    }

    #[test]
    fn failed_provisioning_does_not_install_or_activate_revision() {
        let temp = tempfile::tempdir().unwrap();
        let service = service(temp.path());
        service
            .install(b"secret", "old-install", revision("old"), 0, |_| Ok(()))
            .unwrap();
        service.activate(b"secret", "voice", "old").unwrap();
        let error = service.install(b"secret", "new-install", revision("new"), 10, |phase| {
            if phase == ProvisioningPhase::SmokeTesting {
                Err("Authorization: Bearer abc token=xyz smoke failed".to_owned())
            } else {
                Ok(())
            }
        });
        assert!(matches!(error, Err(DeploymentError::Provisioning(_))));
        assert!(matches!(
            service.activate(b"secret", "voice", "new"),
            Err(DeploymentError::NotInstalled(_))
        ));
        let status = service.status(b"secret", "new-install").unwrap().unwrap();
        assert_eq!(status.phase, ProvisioningPhase::Failed);
        let diagnostic = status.diagnostic.unwrap();
        assert!(!diagnostic.contains("abc"));
        assert!(!diagnostic.contains("xyz"));
    }

    #[test]
    fn install_rejects_missing_verified_content_before_provisioning() {
        let temp = tempfile::tempdir().unwrap();
        let service = service(temp.path());
        let mut missing = revision("missing");
        missing
            .content_digests
            .insert(format!("sha256:{}", "a".repeat(64)));
        let result = service.install(b"secret", "missing-install", missing, 1, |_| Ok(()));
        assert!(matches!(result, Err(DeploymentError::Provisioning(_))));
        assert!(service
            .status(b"secret", "missing-install")
            .unwrap()
            .is_none());
    }

    #[test]
    fn install_fetches_declared_external_assets_before_recording_revision() {
        let temp = tempfile::tempdir().unwrap();
        let bytes = b"external model fixture";
        let asset = AssetDescriptor {
            name: "model.bin".to_owned(),
            digest: sha256_digest(bytes),
            size: bytes.len() as u64,
            cache_key: "model".to_owned(),
            license: None,
            storage: AssetStorage::External {
                source: "https://models.example/model.bin".to_owned(),
                revision: "v1".to_owned(),
                credentials: CredentialPolicy::Forbidden,
            },
        };
        let content = ContentStore::open(temp.path().join("cas")).unwrap();
        let service = DeploymentService::new(
            TokenAuthenticator::new(b"secret"),
            service_capabilities(),
            content.clone(),
            ActivationRegistry::open(temp.path().join("state")).unwrap(),
        )
        .with_external_asset_transport(Arc::new(FixtureTransport(bytes.to_vec())));
        let revision = DeploymentRevision {
            bundle_digest: format!("sha256:{}", "b".repeat(64)),
            variant_digest: "variant".to_owned(),
            manifest_digest: None,
            content_digests: BTreeSet::new(),
            external_assets: vec![asset.clone()],
            native_runtime: None,
        };

        service
            .install(b"secret", "external-install", revision, 0, |_| Ok(()))
            .unwrap();
        assert!(content.contains(&asset.digest));
        assert_eq!(
            service
                .status(b"secret", "external-install")
                .unwrap()
                .unwrap()
                .phase,
            ProvisioningPhase::Ready
        );
    }

    struct FixtureTransport(Vec<u8>);

    impl ExternalAssetTransport for FixtureTransport {
        fn open(
            &self,
            _asset: &AssetDescriptor,
            _offset: u64,
        ) -> Result<Box<dyn Read>, DeploymentError> {
            Ok(Box::new(Cursor::new(self.0.clone())))
        }
    }

    #[test]
    fn diagnostics_redact_url_credentials_and_control_output() {
        let diagnostic = bounded_diagnostic(
            "fetch https://user:password@example.test/model\nsecret=kept\tcontinuation",
        );
        assert_eq!(diagnostic, "fetch [REDACTED] secret=kept continuation");
        assert!(!diagnostic.contains('\n'));
        assert!(!diagnostic.contains("password"));
    }

    #[test]
    fn diagnostics_are_bounded() {
        let diagnostic = bounded_diagnostic(&"x ".repeat(2_000));
        assert!(diagnostic.len() <= 1024);
    }

    fn service_capabilities() -> RuntimeCapabilities {
        RuntimeCapabilities {
            schema_version: remotemedia_bundle::BUNDLE_SCHEMA_VERSION.to_owned(),
            os: "linux".to_owned(),
            architecture: "amd64".to_owned(),
            native_abi: None,
            manifest_schemas: vec!["v1".to_owned()],
            plugin_abi: CompatibilityRange {
                minimum: "0.4.0".to_owned(),
                maximum_exclusive: None,
            },
            python: Vec::new(),
            accelerators: vec![AcceleratorBackend::Cpu],
            memory_bytes: 1024,
            available_cache_bytes: 1024,
            media_devices: Vec::new(),
            runtime_features: Vec::new(),
        }
    }
}
