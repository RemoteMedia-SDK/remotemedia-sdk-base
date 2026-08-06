use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use remotemedia_bundle::{canonical_json, AssetDescriptor, NativeRuntimeClosure};
use serde::{Deserialize, Serialize};

use crate::{ContentStore, DeploymentError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentRevision {
    pub bundle_digest: String,
    pub variant_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_digest: Option<String>,
    #[serde(default)]
    pub content_digests: BTreeSet<String>,
    #[serde(default)]
    pub external_assets: Vec<AssetDescriptor>,
    /// Every asset declared by the bundle lock (embedded and external).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assets: Vec<AssetDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_runtime: Option<NativeRuntimeClosure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentInfo {
    pub name: String,
    pub active_bundle_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_bundle_digest: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegistryState {
    #[serde(default)]
    installed: BTreeMap<String, DeploymentRevision>,
    #[serde(default)]
    deployments: BTreeMap<String, DeploymentState>,
    #[serde(default)]
    pinned: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeploymentState {
    active: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous: Option<String>,
}

pub struct ActivationRegistry {
    state_path: PathBuf,
    state: RegistryState,
}

impl ActivationRegistry {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, DeploymentError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let state_path = root.join("deployments.json");
        let state = if state_path.exists() {
            serde_json::from_slice(&fs::read(&state_path)?)?
        } else {
            RegistryState::default()
        };
        Ok(Self { state_path, state })
    }

    pub fn record_installed(
        &mut self,
        revision: DeploymentRevision,
    ) -> Result<(), DeploymentError> {
        self.state
            .installed
            .insert(revision.bundle_digest.clone(), revision);
        self.persist()
    }

    pub fn activate(&mut self, name: &str, bundle_digest: &str) -> Result<(), DeploymentError> {
        validate_name(name)?;
        if !self.state.installed.contains_key(bundle_digest) {
            return Err(DeploymentError::NotInstalled(bundle_digest.to_owned()));
        }
        let previous = self
            .state
            .deployments
            .get(name)
            .map(|deployment| deployment.active.clone())
            .filter(|active| active != bundle_digest);
        self.state.deployments.insert(
            name.to_owned(),
            DeploymentState {
                active: bundle_digest.to_owned(),
                previous,
            },
        );
        self.persist()
    }

    pub fn rollback(&mut self, name: &str) -> Result<String, DeploymentError> {
        validate_name(name)?;
        let deployment = self
            .state
            .deployments
            .get_mut(name)
            .ok_or_else(|| DeploymentError::NoPreviousRevision(name.to_owned()))?;
        let previous = deployment
            .previous
            .take()
            .ok_or_else(|| DeploymentError::NoPreviousRevision(name.to_owned()))?;
        let old_active = std::mem::replace(&mut deployment.active, previous.clone());
        deployment.previous = Some(old_active);
        self.persist()?;
        Ok(previous)
    }

    pub fn active(&self, name: &str) -> Option<&DeploymentRevision> {
        let digest = &self.state.deployments.get(name)?.active;
        self.state.installed.get(digest)
    }

    pub fn list(&self) -> Vec<DeploymentInfo> {
        self.state
            .deployments
            .iter()
            .map(|(name, deployment)| DeploymentInfo {
                name: name.clone(),
                active_bundle_digest: deployment.active.clone(),
                previous_bundle_digest: deployment.previous.clone(),
            })
            .collect()
    }

    pub fn is_installed(&self, bundle_digest: &str) -> bool {
        self.state.installed.contains_key(bundle_digest)
    }

    pub fn pin(&mut self, bundle_digest: &str) -> Result<(), DeploymentError> {
        if !self.state.installed.contains_key(bundle_digest) {
            return Err(DeploymentError::NotInstalled(bundle_digest.to_owned()));
        }
        self.state.pinned.insert(bundle_digest.to_owned());
        self.persist()
    }

    pub fn unpin(&mut self, bundle_digest: &str) -> Result<(), DeploymentError> {
        self.state.pinned.remove(bundle_digest);
        self.persist()
    }

    pub fn garbage_collect(
        &mut self,
        content: &ContentStore,
    ) -> Result<Vec<String>, DeploymentError> {
        let retained_revisions = self.retained_revisions();
        self.state
            .installed
            .retain(|digest, _| retained_revisions.contains(digest));
        let mut retained_content = BTreeSet::new();
        for revision in self.state.installed.values() {
            retained_content.extend(revision.content_digests.iter().cloned());
        }
        let removed = content.remove_unreferenced(&retained_content)?;
        self.persist()?;
        Ok(removed)
    }

    fn retained_revisions(&self) -> BTreeSet<String> {
        let mut retained = self.state.pinned.clone();
        for deployment in self.state.deployments.values() {
            retained.insert(deployment.active.clone());
            if let Some(previous) = &deployment.previous {
                retained.insert(previous.clone());
            }
        }
        retained
    }

    fn persist(&self) -> Result<(), DeploymentError> {
        let bytes = canonical_json(&self.state)?;
        let temporary = self.state_path.with_extension("json.new");
        fs::write(&temporary, bytes)?;
        File::open(&temporary)?.sync_all()?;
        fs::rename(&temporary, &self.state_path)?;
        sync_parent(&self.state_path)?;
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), DeploymentError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(DeploymentError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), DeploymentError> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use remotemedia_bundle::{sha256_digest, DescriptorIdentity};

    use super::*;

    fn install_blob(store: &ContentStore, bytes: &[u8]) -> String {
        let digest = sha256_digest(bytes);
        let mut upload = store
            .begin_upload(DescriptorIdentity {
                digest: digest.clone(),
                size: bytes.len() as u64,
            })
            .unwrap();
        upload.append(0, bytes).unwrap();
        upload.finish().unwrap();
        digest
    }

    fn revision(bundle: &str, content: &str) -> DeploymentRevision {
        DeploymentRevision {
            bundle_digest: bundle.to_owned(),
            variant_digest: format!("{bundle}-variant"),
            manifest_digest: None,
            content_digests: BTreeSet::from([content.to_owned()]),
            external_assets: Vec::new(),
            assets: Vec::new(),
            native_runtime: None,
        }
    }

    #[test]
    fn activation_is_atomic_and_rollback_retains_previous() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path().join("cas")).unwrap();
        let old_blob = install_blob(&store, b"old");
        let new_blob = install_blob(&store, b"new");
        let mut registry = ActivationRegistry::open(temp.path().join("state")).unwrap();
        registry
            .record_installed(revision("old", &old_blob))
            .unwrap();
        registry.activate("voice", "old").unwrap();

        assert!(registry.activate("voice", "not-installed").is_err());
        assert_eq!(registry.active("voice").unwrap().bundle_digest, "old");

        registry
            .record_installed(revision("new", &new_blob))
            .unwrap();
        registry.activate("voice", "new").unwrap();
        assert_eq!(registry.active("voice").unwrap().bundle_digest, "new");
        assert_eq!(registry.rollback("voice").unwrap(), "old");
        assert_eq!(registry.active("voice").unwrap().bundle_digest, "old");
    }

    #[test]
    fn lists_named_deployments_without_exposing_live_session_state() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path().join("cas")).unwrap();
        let digest = install_blob(&store, b"bundle");
        let mut registry = ActivationRegistry::open(temp.path().join("state")).unwrap();
        registry
            .record_installed(revision("bundle", &digest))
            .unwrap();
        registry.activate("voice", "bundle").unwrap();

        assert_eq!(
            registry.list(),
            vec![DeploymentInfo {
                name: "voice".to_owned(),
                active_bundle_digest: "bundle".to_owned(),
                previous_bundle_digest: None,
            }]
        );
    }

    #[test]
    fn garbage_collection_never_removes_active_previous_or_pinned_content() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path().join("cas")).unwrap();
        let old_blob = install_blob(&store, b"old");
        let active_blob = install_blob(&store, b"active");
        let pinned_blob = install_blob(&store, b"pinned");
        let unused_blob = install_blob(&store, b"unused");
        let mut registry = ActivationRegistry::open(temp.path().join("state")).unwrap();
        for (bundle, blob) in [
            ("old", &old_blob),
            ("active", &active_blob),
            ("pinned", &pinned_blob),
            ("unused", &unused_blob),
        ] {
            registry.record_installed(revision(bundle, blob)).unwrap();
        }
        registry.activate("voice", "old").unwrap();
        registry.activate("voice", "active").unwrap();
        registry.pin("pinned").unwrap();
        let removed = registry.garbage_collect(&store).unwrap();
        assert_eq!(removed, vec![unused_blob]);
        assert!(store.contains(&old_blob));
        assert!(store.contains(&active_blob));
        assert!(store.contains(&pinned_blob));
    }
}
