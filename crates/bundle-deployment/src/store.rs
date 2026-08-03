use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use remotemedia_bundle::{
    sha256_digest, AssetDescriptor, AssetStorage, CredentialPolicy, DescriptorIdentity,
    NativeRuntimeClosure, NativeRuntimeFileMode,
};
use reqwest::header::{HeaderValue, AUTHORIZATION, RANGE};

use crate::DeploymentError;

#[derive(Debug, Clone)]
pub struct ContentStore {
    root: PathBuf,
}

/// Opens an external asset stream at a verified-cache offset.
///
/// Implementations must not write directly to the content store. Every byte is
/// appended through [`UploadSession`] so size and SHA-256 verification remain
/// the sole publication path.
pub trait ExternalAssetTransport {
    fn open(&self, asset: &AssetDescriptor, offset: u64) -> Result<Box<dyn Read>, DeploymentError>;
}

/// HTTPS and OCI-registry external asset transport using Rustls.
///
/// OCI descriptors use `oci://REGISTRY/REPOSITORY` sources and a digest-pinned
/// `revision`; the transport retrieves `/v2/REPOSITORY/blobs/REVISION`.
pub struct ReqwestExternalAssetTransport {
    client: reqwest::blocking::Client,
    bearer_token: Option<HeaderValue>,
}

impl ReqwestExternalAssetTransport {
    pub fn new() -> Result<Self, DeploymentError> {
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| DeploymentError::ExternalAssetFetch(error.to_string()))?;
        Ok(Self {
            client,
            bearer_token: None,
        })
    }

    /// Configures a target-held bearer credential for assets that explicitly
    /// allow target-supplied credentials. The credential is never persisted.
    pub fn with_bearer_token(mut self, token: impl AsRef<str>) -> Result<Self, DeploymentError> {
        let value = HeaderValue::from_str(&format!("Bearer {}", token.as_ref()))
            .map_err(|_| DeploymentError::ExternalAssetFetch("invalid bearer token".to_owned()))?;
        self.bearer_token = Some(value);
        Ok(self)
    }

    fn authorization(&self, asset: &AssetDescriptor) -> Option<&HeaderValue> {
        matches!(
            asset.storage,
            AssetStorage::External {
                credentials: CredentialPolicy::TargetMaySupply,
                ..
            }
        )
        .then_some(())
        .and(self.bearer_token.as_ref())
    }

    fn request_url(asset: &AssetDescriptor) -> Result<reqwest::Url, DeploymentError> {
        let AssetStorage::External {
            source, revision, ..
        } = &asset.storage
        else {
            return Err(DeploymentError::InvalidAssetSource(
                "embedded assets do not have a fetch URL".to_owned(),
            ));
        };
        if let Some(reference) = source.strip_prefix("oci://") {
            validate_digest(revision)?;
            let (registry, repository) = reference.split_once('/').ok_or_else(|| {
                DeploymentError::InvalidAssetSource(
                    "OCI asset source must include registry and repository".to_owned(),
                )
            })?;
            if registry.is_empty()
                || repository.is_empty()
                || reference.contains('@')
                || reference.contains('?')
                || reference.contains('#')
            {
                return Err(DeploymentError::InvalidAssetSource(source.clone()));
            }
            return format!("https://{registry}/v2/{repository}/blobs/{revision}")
                .parse()
                .map_err(|_| DeploymentError::InvalidAssetSource(source.clone()));
        }

        let url: reqwest::Url = source
            .parse()
            .map_err(|_| DeploymentError::InvalidAssetSource(source.clone()))?;
        if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
            return Err(DeploymentError::InvalidAssetSource(source.clone()));
        }
        Ok(url)
    }
}

impl ExternalAssetTransport for ReqwestExternalAssetTransport {
    fn open(&self, asset: &AssetDescriptor, offset: u64) -> Result<Box<dyn Read>, DeploymentError> {
        let url = Self::request_url(asset)?;
        let mut request = self.client.get(url);
        if let Some(token) = self.authorization(asset) {
            request = request.header(AUTHORIZATION, token.clone());
        }
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
        }
        let response = request
            .send()
            .map_err(|error| DeploymentError::ExternalAssetFetch(error.to_string()))?;
        let expected = if offset == 0 {
            response.status().is_success()
        } else {
            response.status() == reqwest::StatusCode::PARTIAL_CONTENT
        };
        if !expected {
            return Err(DeploymentError::ExternalAssetFetch(format!(
                "unexpected HTTP status {} for external asset",
                response.status()
            )));
        }
        Ok(Box::new(response))
    }
}

impl ContentStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, DeploymentError> {
        let root = root.into();
        fs::create_dir_all(root.join("blobs/sha256"))?;
        fs::create_dir_all(root.join("uploads"))?;
        fs::create_dir_all(root.join("quarantine"))?;
        fs::create_dir_all(root.join("releases"))?;
        Ok(Self { root })
    }

    pub fn contains(&self, digest: &str) -> bool {
        self.blob_path(digest).is_ok_and(|path| path.is_file())
    }

    pub fn missing<'a>(
        &self,
        descriptors: impl IntoIterator<Item = &'a DescriptorIdentity>,
    ) -> Vec<DescriptorIdentity> {
        descriptors
            .into_iter()
            .filter(|descriptor| !self.contains(&descriptor.digest))
            .cloned()
            .collect()
    }

    pub fn begin_upload(
        &self,
        descriptor: DescriptorIdentity,
    ) -> Result<UploadSession, DeploymentError> {
        validate_digest(&descriptor.digest)?;
        let hex = descriptor.digest.trim_start_matches("sha256:");
        let path = self.root.join("uploads").join(format!("{hex}.partial"));
        let offset = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
        if offset > descriptor.size {
            fs::rename(
                &path,
                self.root.join("quarantine").join(format!("{hex}.oversize")),
            )?;
            return Err(DeploymentError::SizeMismatch {
                expected: descriptor.size,
                actual: offset,
            });
        }
        Ok(UploadSession {
            store: self.clone(),
            descriptor,
            path,
            offset,
        })
    }

    /// Begin a resumable download of an immutable external asset.
    ///
    /// The network adapter is responsible for fetching `source` with the
    /// declared credentials policy and appending response chunks to the
    /// returned session. Publication still requires the declared size and
    /// SHA-256 digest, so interrupted or corrupt downloads never enter cache.
    pub fn begin_external_asset(
        &self,
        asset: &AssetDescriptor,
    ) -> Result<UploadSession, DeploymentError> {
        let AssetStorage::External {
            source,
            revision,
            credentials: _,
        } = &asset.storage
        else {
            return Err(DeploymentError::InvalidAssetSource(
                "embedded assets do not need external fetching".to_owned(),
            ));
        };
        if !source.starts_with("https://") && !source.starts_with("oci://") {
            return Err(DeploymentError::InvalidAssetSource(source.clone()));
        }
        if revision.trim().is_empty() || matches!(revision.as_str(), "latest" | "main" | "master") {
            return Err(DeploymentError::InvalidAssetSource(
                "asset revision must be immutable".to_owned(),
            ));
        }
        self.begin_upload(DescriptorIdentity {
            digest: asset.digest.clone(),
            size: asset.size,
        })
    }

    /// Resume an immutable external asset through a transport implementation.
    pub fn fetch_external_asset<T: ExternalAssetTransport + ?Sized>(
        &self,
        asset: &AssetDescriptor,
        transport: &T,
    ) -> Result<PathBuf, DeploymentError> {
        if self.contains(&asset.digest) {
            return self.blob_path(&asset.digest);
        }
        let mut upload = self.begin_external_asset(asset)?;
        let mut response = transport.open(asset, upload.offset())?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = response.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let offset = upload.offset();
            upload.append(offset, &buffer[..count])?;
        }
        upload.finish()
    }

    pub fn read(&self, digest: &str) -> Result<Vec<u8>, DeploymentError> {
        Ok(fs::read(self.blob_path(digest)?)?)
    }

    pub fn native_runtime_release(&self, bundle_digest: &str) -> Result<PathBuf, DeploymentError> {
        validate_digest(bundle_digest)?;
        Ok(self
            .root
            .join("releases")
            .join(bundle_digest.trim_start_matches("sha256:")))
    }

    /// Materialize a verified native runtime closure below a release directory
    /// named by the immutable bundle digest. Files are copied from CAS so their
    /// release-local permissions can never alter content-addressed blobs.
    pub fn materialize_native_runtime(
        &self,
        bundle_digest: &str,
        closure: &NativeRuntimeClosure,
    ) -> Result<PathBuf, DeploymentError> {
        validate_digest(bundle_digest)?;
        let release_name = bundle_digest.trim_start_matches("sha256:");
        let releases = self.root.join("releases");
        let release = releases.join(release_name);
        if release.is_dir() {
            return Ok(release);
        }

        let temporary = releases.join(format!(".{release_name}.new-{}", std::process::id()));
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;

        let result = (|| {
            let mut materialized = std::collections::BTreeSet::new();
            for file in &closure.files {
                validate_digest(&file.digest)?;
                let relative = validated_runtime_path(&file.path)?;
                if !materialized.insert(relative.clone()) {
                    return Err(DeploymentError::InvalidRuntimePath(file.path.clone()));
                }
                let source = self.blob_path(&file.digest)?;
                let metadata = fs::metadata(&source)?;
                if metadata.len() != file.size {
                    return Err(DeploymentError::SizeMismatch {
                        expected: file.size,
                        actual: metadata.len(),
                    });
                }
                let destination = temporary.join(&relative);
                let parent = destination.parent().expect("relative runtime path has parent");
                fs::create_dir_all(parent)?;
                fs::copy(source, &destination)?;
                File::open(&destination)?.sync_all()?;
                set_runtime_mode(&destination, file.mode)?;
            }
            for symlink in &closure.symlinks {
                let relative = validated_runtime_path(&symlink.path)?;
                let target = validated_runtime_symlink_target(&symlink.target)?;
                let parent = relative.parent().unwrap_or_else(|| Path::new(""));
                if !materialized.contains(&parent.join(&target)) {
                    return Err(DeploymentError::InvalidRuntimePath(symlink.path.clone()));
                }
                let destination = temporary.join(&relative);
                if destination.exists() || destination.is_symlink() {
                    return Err(DeploymentError::InvalidRuntimePath(symlink.path.clone()));
                }
                create_runtime_symlink(&target, &destination)?;
            }
            sync_parent(&temporary.join("placeholder"))?;
            Ok(())
        })();

        if let Err(error) = result {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
        match fs::rename(&temporary, &release) {
            Ok(()) => {
                sync_parent(&release)?;
                Ok(release)
            }
            Err(_) if release.is_dir() => {
                fs::remove_dir_all(&temporary)?;
                Ok(release)
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&temporary);
                Err(error.into())
            }
        }
    }

    pub fn remove_unreferenced(
        &self,
        keep: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<String>, DeploymentError> {
        let directory = self.root.join("blobs/sha256");
        let mut removed = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let digest = format!("sha256:{}", entry.file_name().to_string_lossy());
            if !keep.contains(&digest) {
                fs::remove_file(entry.path())?;
                removed.push(digest);
            }
        }
        removed.sort();
        Ok(removed)
    }

    fn blob_path(&self, digest: &str) -> Result<PathBuf, DeploymentError> {
        validate_digest(digest)?;
        Ok(self
            .root
            .join("blobs/sha256")
            .join(digest.trim_start_matches("sha256:")))
    }
}

pub struct UploadSession {
    store: ContentStore,
    descriptor: DescriptorIdentity,
    path: PathBuf,
    offset: u64,
}

impl UploadSession {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<u64, DeploymentError> {
        if offset != self.offset {
            return Err(DeploymentError::OffsetMismatch {
                expected: self.offset,
                actual: offset,
            });
        }
        let new_size = self.offset.saturating_add(bytes.len() as u64);
        if new_size > self.descriptor.size {
            return Err(DeploymentError::SizeMismatch {
                expected: self.descriptor.size,
                actual: new_size,
            });
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&self.path)?;
        file.seek(SeekFrom::Start(self.offset))?;
        file.write_all(bytes)?;
        file.sync_data()?;
        self.offset = new_size;
        Ok(self.offset)
    }

    pub fn finish(self) -> Result<PathBuf, DeploymentError> {
        if self.offset != self.descriptor.size {
            return Err(DeploymentError::SizeMismatch {
                expected: self.descriptor.size,
                actual: self.offset,
            });
        }
        let mut file = File::open(&self.path)?;
        let mut bytes = Vec::with_capacity(self.offset as usize);
        file.read_to_end(&mut bytes)?;
        let actual = sha256_digest(&bytes);
        if actual != self.descriptor.digest {
            let quarantine = quarantine_path(&self.store.root, &self.path, "digest-mismatch");
            fs::rename(&self.path, quarantine)?;
            return Err(DeploymentError::DigestMismatch {
                expected: self.descriptor.digest,
                actual,
            });
        }
        let destination = self.store.blob_path(&self.descriptor.digest)?;
        if destination.exists() {
            fs::remove_file(&self.path)?;
        } else {
            fs::rename(&self.path, &destination)?;
        }
        sync_parent(&destination)?;
        Ok(destination)
    }
}

fn validate_digest(digest: &str) -> Result<(), DeploymentError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(DeploymentError::InvalidDigest(digest.to_owned()));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DeploymentError::InvalidDigest(digest.to_owned()));
    }
    Ok(())
}

fn validated_runtime_path(value: &str) -> Result<PathBuf, DeploymentError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DeploymentError::InvalidRuntimePath(value.to_owned()));
    }
    Ok(path.to_owned())
}

fn validated_runtime_symlink_target(value: &str) -> Result<PathBuf, DeploymentError> {
    let path = validated_runtime_path(value)?;
    if path.components().count() != 1 {
        return Err(DeploymentError::InvalidRuntimePath(value.to_owned()));
    }
    Ok(path)
}

#[cfg(unix)]
fn set_runtime_mode(path: &Path, mode: NativeRuntimeFileMode) -> Result<(), DeploymentError> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = match mode {
        NativeRuntimeFileMode::ReadOnly => 0o444,
        NativeRuntimeFileMode::Executable => 0o555,
    };
    fs::set_permissions(path, fs::Permissions::from_mode(permissions))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_runtime_mode(_path: &Path, _mode: NativeRuntimeFileMode) -> Result<(), DeploymentError> {
    Ok(())
}

#[cfg(unix)]
fn create_runtime_symlink(target: &Path, destination: &Path) -> Result<(), DeploymentError> {
    std::os::unix::fs::symlink(target, destination)?;
    Ok(())
}

#[cfg(not(unix))]
fn create_runtime_symlink(_target: &Path, _destination: &Path) -> Result<(), DeploymentError> {
    Err(DeploymentError::Provisioning(
        "native runtime symlinks are unsupported on this target".to_owned(),
    ))
}

fn quarantine_path(root: &Path, upload: &Path, reason: &str) -> PathBuf {
    let name = upload.file_name().unwrap_or_default().to_string_lossy();
    root.join("quarantine").join(format!("{name}.{reason}"))
}

fn sync_parent(path: &Path) -> Result<(), DeploymentError> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    use remotemedia_bundle::sha256_digest;

    use super::*;

    #[test]
    fn interrupted_upload_resumes_and_publishes_only_after_verification() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let bytes = b"portable bundle blob";
        let descriptor = DescriptorIdentity {
            digest: sha256_digest(bytes),
            size: bytes.len() as u64,
        };
        let mut first = store.begin_upload(descriptor.clone()).unwrap();
        first.append(0, &bytes[..8]).unwrap();
        drop(first);
        assert!(!store.contains(&descriptor.digest));

        let mut resumed = store.begin_upload(descriptor.clone()).unwrap();
        assert_eq!(resumed.offset(), 8);
        resumed.append(8, &bytes[8..]).unwrap();
        resumed.finish().unwrap();
        assert_eq!(store.read(&descriptor.digest).unwrap(), bytes);
        assert!(store.missing([&descriptor]).is_empty());
    }

    #[test]
    fn corrupt_upload_never_enters_verified_cache() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let descriptor = DescriptorIdentity {
            digest: format!("sha256:{}", "a".repeat(64)),
            size: 3,
        };
        let mut upload = store.begin_upload(descriptor.clone()).unwrap();
        upload.append(0, b"bad").unwrap();
        assert!(matches!(
            upload.finish(),
            Err(DeploymentError::DigestMismatch { .. })
        ));
        assert!(!store.contains(&descriptor.digest));
    }

    #[test]
    fn external_asset_download_requires_authorized_immutable_source() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let bytes = b"model asset";
        let descriptor = remotemedia_bundle::AssetDescriptor {
            name: "model.bin".to_owned(),
            digest: sha256_digest(bytes),
            size: bytes.len() as u64,
            cache_key: "model-cache".to_owned(),
            license: None,
            storage: remotemedia_bundle::AssetStorage::External {
                source: "https://models.example/model.bin".to_owned(),
                revision: "sha256:revision".to_owned(),
                credentials: remotemedia_bundle::CredentialPolicy::Forbidden,
            },
        };
        let mut upload = store.begin_external_asset(&descriptor).unwrap();
        assert_eq!(upload.offset(), 0);
        upload.append(0, bytes).unwrap();
        upload.finish().unwrap();
        assert_eq!(store.read(&descriptor.digest).unwrap(), bytes);

        let mut mutable = descriptor.clone();
        if let remotemedia_bundle::AssetStorage::External { revision, .. } = &mut mutable.storage {
            *revision = "latest".to_owned();
        }
        assert!(matches!(
            store.begin_external_asset(&mutable),
            Err(DeploymentError::InvalidAssetSource(_))
        ));

        let mut unauthorized = descriptor;
        if let remotemedia_bundle::AssetStorage::External { source, .. } = &mut unauthorized.storage
        {
            *source = "http://models.example/model.bin".to_owned();
        }
        assert!(matches!(
            store.begin_external_asset(&unauthorized),
            Err(DeploymentError::InvalidAssetSource(_))
        ));
    }

    #[test]
    fn embedded_assets_cannot_start_external_downloads() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let asset = remotemedia_bundle::AssetDescriptor {
            name: "embedded.bin".to_owned(),
            digest: format!("sha256:{}", "a".repeat(64)),
            size: 1,
            cache_key: "embedded".to_owned(),
            license: None,
            storage: remotemedia_bundle::AssetStorage::Embedded,
        };
        assert!(matches!(
            store.begin_external_asset(&asset),
            Err(DeploymentError::InvalidAssetSource(_))
        ));
    }

    #[test]
    fn materializes_release_local_native_files_and_soname_aliases() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let bytes = b"native sidecar";
        let digest = sha256_digest(bytes);
        let mut upload = store
            .begin_upload(DescriptorIdentity {
                digest: digest.clone(),
                size: bytes.len() as u64,
            })
            .unwrap();
        upload.append(0, bytes).unwrap();
        upload.finish().unwrap();
        let bundle_digest = format!("sha256:{}", "b".repeat(64));
        let closure = NativeRuntimeClosure {
            files: vec![remotemedia_bundle::NativeRuntimeFile {
                digest,
                size: bytes.len() as u64,
                path: "lib/libwhisper.so.1.9.1".to_owned(),
                mode: NativeRuntimeFileMode::Executable,
            }],
            symlinks: vec![remotemedia_bundle::NativeRuntimeSymlink {
                path: "lib/libwhisper.so.1".to_owned(),
                target: "libwhisper.so.1.9.1".to_owned(),
            }],
        };

        let release = store
            .materialize_native_runtime(&bundle_digest, &closure)
            .unwrap();
        assert_eq!(fs::read(release.join("lib/libwhisper.so.1")).unwrap(), bytes);
        assert_eq!(
            std::fs::read_link(release.join("lib/libwhisper.so.1")).unwrap(),
            PathBuf::from("libwhisper.so.1.9.1")
        );
        assert!(matches!(
            store.materialize_native_runtime(
                &format!("sha256:{}", "c".repeat(64)),
                &NativeRuntimeClosure {
                    files: vec![remotemedia_bundle::NativeRuntimeFile {
                        digest: sha256_digest(b"bad"),
                        size: 3,
                        path: "../outside".to_owned(),
                        mode: NativeRuntimeFileMode::ReadOnly,
                    }],
                    symlinks: Vec::new(),
                },
            ),
            Err(DeploymentError::InvalidRuntimePath(path)) if path == "../outside"
        ));
    }

    #[test]
    fn external_asset_transport_resumes_through_verified_uploads() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let bytes = b"resumable external model";
        let asset = external_asset(bytes);
        let mut partial = store.begin_external_asset(&asset).unwrap();
        partial.append(0, &bytes[..9]).unwrap();
        drop(partial);

        let offsets = Arc::new(Mutex::new(Vec::new()));
        let transport = TestTransport {
            bytes: bytes.to_vec(),
            offsets: offsets.clone(),
        };
        store.fetch_external_asset(&asset, &transport).unwrap();

        assert_eq!(*offsets.lock().unwrap(), vec![9]);
        assert_eq!(store.read(&asset.digest).unwrap(), bytes);
    }

    #[test]
    fn oci_sources_resolve_to_digest_pinned_registry_blob_urls() {
        let bytes = b"oci model";
        let mut asset = external_asset(bytes);
        asset.storage = AssetStorage::External {
            source: "oci://registry.example/models/tiny".to_owned(),
            revision: asset.digest.clone(),
            credentials: remotemedia_bundle::CredentialPolicy::Forbidden,
        };
        assert_eq!(
            ReqwestExternalAssetTransport::request_url(&asset)
                .unwrap()
                .as_str(),
            format!(
                "https://registry.example/v2/models/tiny/blobs/{}",
                asset.digest
            )
        );
    }

    #[test]
    fn bearer_credentials_are_accepted_only_for_opt_in_assets() {
        let transport = ReqwestExternalAssetTransport::new()
            .unwrap()
            .with_bearer_token("target-token")
            .unwrap();
        let mut asset = external_asset(b"credential policy");
        assert!(transport.authorization(&asset).is_none());
        if let AssetStorage::External { credentials, .. } = &mut asset.storage {
            *credentials = remotemedia_bundle::CredentialPolicy::TargetMaySupply;
        }
        assert_eq!(
            transport.authorization(&asset).unwrap(),
            &HeaderValue::from_static("Bearer target-token")
        );
    }

    fn external_asset(bytes: &[u8]) -> AssetDescriptor {
        AssetDescriptor {
            name: "model.bin".to_owned(),
            digest: sha256_digest(bytes),
            size: bytes.len() as u64,
            cache_key: "model-cache".to_owned(),
            license: None,
            storage: AssetStorage::External {
                source: "https://models.example/model.bin".to_owned(),
                revision: "v1.2.3".to_owned(),
                credentials: remotemedia_bundle::CredentialPolicy::Forbidden,
            },
        }
    }

    struct TestTransport {
        bytes: Vec<u8>,
        offsets: Arc<Mutex<Vec<u64>>>,
    }

    impl ExternalAssetTransport for TestTransport {
        fn open(
            &self,
            _asset: &AssetDescriptor,
            offset: u64,
        ) -> Result<Box<dyn Read>, DeploymentError> {
            self.offsets.lock().unwrap().push(offset);
            Ok(Box::new(Cursor::new(
                self.bytes[offset as usize..].to_vec(),
            )))
        }
    }
}
