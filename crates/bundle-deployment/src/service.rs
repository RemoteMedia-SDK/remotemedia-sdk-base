use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use remotemedia_bundle::{
    sha256_digest, DescriptorIdentity, InstallStatus, PreflightReport, ProvisioningPhase,
    RuntimeCapabilities, VariantCandidate,
};

use crate::{
    preflight, ActivationRegistry, ContentStore, DeploymentError, DeploymentRevision, UploadSession,
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
        }
    }

    pub fn capabilities(&self, token: &[u8]) -> Result<RuntimeCapabilities, DeploymentError> {
        self.authenticator.authorize(token)?;
        Ok(self.capabilities.clone())
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
    let lowercase = value.to_ascii_lowercase();
    if lowercase.contains("authorization:")
        || lowercase.contains("token=")
        || lowercase.contains("bearer ")
    {
        return "[REDACTED] provisioning failed".to_owned();
    }
    let redacted = value
        .split_whitespace()
        .map(|word| {
            if word.to_ascii_lowercase().contains("token=")
                || word.to_ascii_lowercase().contains("authorization:")
            {
                "[REDACTED]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    redacted.chars().take(1024).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use remotemedia_bundle::{AcceleratorBackend, CompatibilityRange};

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
            content_digests: BTreeSet::new(),
        }
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
}
