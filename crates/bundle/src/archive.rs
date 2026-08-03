use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{Read, Write};
use std::path::{Component, Path};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tar::{EntryType, Header};

use crate::canonical::{canonical_json, sha256_digest, validate_digest};
use crate::media_types;
use crate::oci::{Descriptor, OciImageLayout, OciImageManifest, OciIndex};
use crate::schema::{BundleConfig, BundleLock, TargetRequirements};
use crate::{BUNDLE_SCHEMA_VERSION, OCI_LAYOUT_VERSION};

const OCI_LAYOUT_PATH: &str = "oci-layout";
const INDEX_PATH: &str = "index.json";
const BLOBS_PREFIX: &str = "blobs/sha256/";

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("bundle I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("bundle JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("archive path is unsafe: {0}")]
    UnsafePath(String),
    #[error("archive contains duplicate path: {0}")]
    DuplicatePath(String),
    #[error("archive entry is not a regular file: {0}")]
    UnsupportedEntry(String),
    #[error("archive limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("required OCI entry is missing: {0}")]
    MissingEntry(String),
    #[error("unsupported OCI image layout version: {0}")]
    UnsupportedLayout(String),
    #[error("unsupported OCI schema or media type: {0}")]
    UnsupportedSchema(String),
    #[error("descriptor is invalid: {0}")]
    InvalidDescriptor(String),
    #[error("descriptor content mismatch: {0}")]
    DigestMismatch(String),
    #[error("descriptor conflicts with another reference to the same digest: {0}")]
    ConflictingDescriptor(String),
    #[error("JSON blob is not canonical: {0}")]
    NonCanonicalJson(String),
    #[error("archive contains unreferenced blob: {0}")]
    UnreferencedBlob(String),
}

#[derive(Debug, Clone, Copy)]
pub struct BundleLimits {
    pub max_entries: usize,
    pub max_entry_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_total_bytes: u64,
}

impl Default for BundleLimits {
    fn default() -> Self {
        Self {
            max_entries: 16_384,
            max_entry_bytes: 16 * 1024 * 1024 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_total_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
struct Blob {
    media_type: String,
    bytes: Vec<u8>,
}

/// An in-memory OCI image layout ready to be written as `.rmpkg`.
#[derive(Debug, Clone, Default)]
pub struct BundleLayout {
    pub index: OciIndex,
    blobs: BTreeMap<String, Blob>,
}

impl BundleLayout {
    pub fn new() -> Self {
        Self {
            index: OciIndex::default(),
            blobs: BTreeMap::new(),
        }
    }

    pub fn add_blob(&mut self, media_type: impl Into<String>, bytes: Vec<u8>) -> Descriptor {
        let media_type = media_type.into();
        let digest = sha256_digest(&bytes);
        let size = bytes.len() as u64;
        self.blobs.entry(digest.clone()).or_insert_with(|| Blob {
            media_type: media_type.clone(),
            bytes,
        });
        Descriptor {
            media_type,
            digest,
            size,
            platform: None,
            annotations: BTreeMap::new(),
        }
    }

    pub fn add_json_blob<T: Serialize>(
        &mut self,
        media_type: impl Into<String>,
        value: &T,
    ) -> Result<Descriptor, BundleError> {
        let media_type = media_type.into();
        Ok(self.add_blob(media_type, canonical_json(value)?))
    }

    pub fn add_variant(
        &mut self,
        manifest: &OciImageManifest,
        platform: crate::oci::OciPlatform,
    ) -> Result<Descriptor, BundleError> {
        let mut descriptor = self.add_json_blob(media_types::OCI_IMAGE_MANIFEST, manifest)?;
        descriptor.platform = Some(platform);
        self.index.manifests.push(descriptor.clone());
        Ok(descriptor)
    }

    pub fn write_rmpkg<W: Write>(&self, writer: W) -> Result<(), BundleError> {
        validate_layout(&self.index, &self.blobs)?;
        let layout = canonical_json(&OciImageLayout::default())?;
        let index = canonical_json(&self.index)?;

        let mut entries = BTreeMap::new();
        entries.insert(OCI_LAYOUT_PATH.to_owned(), layout);
        entries.insert(INDEX_PATH.to_owned(), index);
        for (digest, blob) in &self.blobs {
            let hex = digest
                .strip_prefix("sha256:")
                .ok_or_else(|| BundleError::InvalidDescriptor(digest.clone()))?;
            entries.insert(format!("{BLOBS_PREFIX}{hex}"), blob.bytes.clone());
        }

        let mut encoder = zstd::stream::write::Encoder::new(writer, 19)?;
        encoder.include_checksum(true)?;
        let mut archive = tar::Builder::new(encoder);
        archive.mode(tar::HeaderMode::Deterministic);
        for (path, bytes) in entries {
            append_file(&mut archive, &path, &bytes)?;
        }
        let encoder = archive.into_inner()?;
        encoder.finish()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedBundle {
    pub index: OciIndex,
    pub bundle_digest: String,
    pub blob_count: usize,
    pub total_uncompressed_bytes: u64,
    pub variants: Vec<VerifiedVariant>,
    blobs: BTreeMap<String, Vec<u8>>,
}

impl VerifiedBundle {
    pub fn blob(&self, digest: &str) -> Option<&[u8]> {
        self.blobs.get(digest).map(Vec::as_slice)
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedVariant {
    pub descriptor: Descriptor,
    pub manifest: OciImageManifest,
    pub config: BundleConfig,
    pub lock: BundleLock,
    pub target: TargetRequirements,
}

impl VerifiedBundle {
    pub fn read_rmpkg<R: Read>(reader: R, limits: BundleLimits) -> Result<Self, BundleError> {
        let decoder = zstd::stream::read::Decoder::new(reader)?;
        let mut archive = tar::Archive::new(decoder);
        let mut files = BTreeMap::<String, Vec<u8>>::new();
        let mut total = 0_u64;

        for (entry_count, entry) in archive.entries()?.enumerate() {
            if entry_count >= limits.max_entries {
                return Err(BundleError::LimitExceeded(format!(
                    "more than {} archive entries",
                    limits.max_entries
                )));
            }
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            let path_text = validate_archive_path(&path)?;
            if entry.header().entry_type() != EntryType::Regular {
                return Err(BundleError::UnsupportedEntry(path_text));
            }
            if files.contains_key(&path_text) {
                return Err(BundleError::DuplicatePath(path_text));
            }
            let declared_size = entry.header().size()?;
            let applicable_limit = if path_text == OCI_LAYOUT_PATH || path_text == INDEX_PATH {
                limits.max_metadata_bytes
            } else {
                limits.max_entry_bytes
            };
            if declared_size > applicable_limit {
                return Err(BundleError::LimitExceeded(format!(
                    "{path_text} declares {declared_size} bytes"
                )));
            }
            total = total.checked_add(declared_size).ok_or_else(|| {
                BundleError::LimitExceeded("uncompressed byte count overflow".to_owned())
            })?;
            if total > limits.max_total_bytes {
                return Err(BundleError::LimitExceeded(format!(
                    "more than {} uncompressed bytes",
                    limits.max_total_bytes
                )));
            }
            let mut bytes = Vec::with_capacity(declared_size.min(usize::MAX as u64) as usize);
            entry.read_to_end(&mut bytes)?;
            if bytes.len() as u64 != declared_size {
                return Err(BundleError::DigestMismatch(format!(
                    "{path_text} size differs from tar header"
                )));
            }
            files.insert(path_text, bytes);
        }

        verify_files(files, total, limits)
    }
}

fn append_file<W: Write>(
    archive: &mut tar::Builder<W>,
    path: &str,
    bytes: &[u8],
) -> Result<(), BundleError> {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(bytes.len() as u64);
    header.set_cksum();
    archive.append_data(&mut header, path, bytes)?;
    Ok(())
}

fn validate_archive_path(path: &Path) -> Result<String, BundleError> {
    let text = path
        .to_str()
        .ok_or_else(|| BundleError::UnsafePath("non-UTF-8 path".to_owned()))?;
    if text.is_empty() || text.contains('\\') {
        return Err(BundleError::UnsafePath(text.to_owned()));
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BundleError::UnsafePath(text.to_owned()));
    }
    if text != OCI_LAYOUT_PATH && text != INDEX_PATH {
        let Some(hex) = text.strip_prefix(BLOBS_PREFIX) else {
            return Err(BundleError::UnsafePath(text.to_owned()));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(BundleError::UnsafePath(text.to_owned()));
        }
    }
    Ok(text.to_owned())
}

fn verify_files(
    mut files: BTreeMap<String, Vec<u8>>,
    total: u64,
    limits: BundleLimits,
) -> Result<VerifiedBundle, BundleError> {
    let layout_bytes = files
        .remove(OCI_LAYOUT_PATH)
        .ok_or_else(|| BundleError::MissingEntry(OCI_LAYOUT_PATH.to_owned()))?;
    let index_bytes = files
        .remove(INDEX_PATH)
        .ok_or_else(|| BundleError::MissingEntry(INDEX_PATH.to_owned()))?;
    require_canonical_json::<OciImageLayout>(OCI_LAYOUT_PATH, &layout_bytes)?;
    let layout: OciImageLayout = serde_json::from_slice(&layout_bytes)?;
    if layout.image_layout_version != OCI_LAYOUT_VERSION {
        return Err(BundleError::UnsupportedLayout(layout.image_layout_version));
    }
    require_canonical_json::<OciIndex>(INDEX_PATH, &index_bytes)?;
    let index: OciIndex = serde_json::from_slice(&index_bytes)?;
    validate_index_header(&index)?;

    let mut blobs = BTreeMap::new();
    for (path, bytes) in files {
        let hex = path
            .strip_prefix(BLOBS_PREFIX)
            .expect("path validated above");
        let digest = format!("sha256:{hex}");
        if sha256_digest(&bytes) != digest {
            return Err(BundleError::DigestMismatch(path));
        }
        blobs.insert(digest, bytes);
    }

    let mut descriptors = BTreeMap::<String, (String, u64)>::new();
    let mut reachable = BTreeSet::new();
    let mut queue = VecDeque::new();
    for descriptor in &index.manifests {
        if descriptor.platform.is_none() {
            return Err(BundleError::InvalidDescriptor(format!(
                "variant {} has no platform",
                descriptor.digest
            )));
        }
        queue.push_back((descriptor.clone(), true));
    }

    while let Some((descriptor, is_manifest)) = queue.pop_front() {
        record_descriptor(&descriptor, &mut descriptors)?;
        let bytes = verify_descriptor_blob(&descriptor, &blobs)?;
        reachable.insert(descriptor.digest.clone());
        if media_types::is_json(&descriptor.media_type) {
            require_canonical_value(&descriptor.digest, bytes, limits.max_metadata_bytes)?;
        }
        if is_manifest {
            if descriptor.media_type != media_types::OCI_IMAGE_MANIFEST {
                return Err(BundleError::UnsupportedSchema(descriptor.media_type));
            }
            let manifest: OciImageManifest = serde_json::from_slice(bytes)?;
            if manifest.schema_version != 2
                || manifest.media_type != media_types::OCI_IMAGE_MANIFEST
            {
                return Err(BundleError::UnsupportedSchema(format!(
                    "manifest {}",
                    descriptor.digest
                )));
            }
            queue.push_back((manifest.config, false));
            queue.extend(manifest.layers.into_iter().map(|layer| (layer, false)));
        }
    }

    for digest in blobs.keys() {
        if !reachable.contains(digest) {
            return Err(BundleError::UnreferencedBlob(digest.clone()));
        }
    }

    let variants = verify_variant_contracts(&index, &blobs)?;
    Ok(VerifiedBundle {
        index,
        // OCI identity is the digest of the index that selects every variant.
        bundle_digest: sha256_digest(&index_bytes),
        blob_count: blobs.len(),
        total_uncompressed_bytes: total,
        variants,
        blobs,
    })
}

fn verify_variant_contracts(
    index: &OciIndex,
    blobs: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<VerifiedVariant>, BundleError> {
    let mut verified = Vec::with_capacity(index.manifests.len());
    let mut target_ids = BTreeSet::new();
    for descriptor in &index.manifests {
        let manifest: OciImageManifest =
            serde_json::from_slice(verify_descriptor_blob(descriptor, blobs)?)?;
        let config: BundleConfig =
            serde_json::from_slice(verify_descriptor_blob(&manifest.config, blobs)?)?;
        require_bundle_version("bundle config", &config.schema_version)?;
        let pipeline = unique_layer(&manifest, media_types::PIPELINE_MANIFEST)?;
        let lock_descriptor = unique_layer(&manifest, media_types::LOCKFILE)?;
        let target_descriptor = unique_layer(&manifest, media_types::TARGET_REQUIREMENTS)?;
        if config.manifest_digest != pipeline.digest
            || config.lock_digest != lock_descriptor.digest
            || config.target_digest != target_descriptor.digest
        {
            return Err(BundleError::InvalidDescriptor(format!(
                "variant {} config does not identify its manifest, lock, and target layers",
                descriptor.digest
            )));
        }
        let lock: BundleLock =
            serde_json::from_slice(verify_descriptor_blob(lock_descriptor, blobs)?)?;
        let target: TargetRequirements =
            serde_json::from_slice(verify_descriptor_blob(target_descriptor, blobs)?)?;
        require_bundle_version("lock", &lock.schema_version)?;
        require_bundle_version("target", &target.schema_version)?;
        if lock.manifest_digest != pipeline.digest || lock.target_id != target.target_id {
            return Err(BundleError::InvalidDescriptor(format!(
                "variant {} lock does not match its manifest or target",
                descriptor.digest
            )));
        }
        let platform = descriptor
            .platform
            .as_ref()
            .expect("platform checked above");
        if platform.os != target.os || platform.architecture != target.architecture {
            return Err(BundleError::InvalidDescriptor(format!(
                "variant {} OCI platform does not match target requirements",
                descriptor.digest
            )));
        }
        if !target_ids.insert(target.target_id.clone()) {
            return Err(BundleError::ConflictingDescriptor(format!(
                "duplicate target id {}",
                target.target_id
            )));
        }
        for plugin in &lock.plugins {
            let layer = manifest
                .layers
                .iter()
                .find(|layer| layer.digest == plugin.artifact_digest)
                .ok_or_else(|| BundleError::MissingEntry(plugin.artifact_digest.clone()))?;
            let expected_type = match plugin.kind {
                crate::schema::PluginKind::Native => media_types::NATIVE_PLUGIN,
                crate::schema::PluginKind::PythonWheel => media_types::PYTHON_WHEEL,
            };
            if layer.media_type != expected_type {
                return Err(BundleError::InvalidDescriptor(
                    plugin.artifact_digest.clone(),
                ));
            }
        }
        for asset in &lock.assets {
            if matches!(asset.storage, crate::schema::AssetStorage::Embedded)
                && !manifest.layers.iter().any(|layer| {
                    layer.digest == asset.digest && layer.media_type == media_types::EMBEDDED_ASSET
                })
            {
                return Err(BundleError::MissingEntry(asset.digest.clone()));
            }
        }
        verified.push(VerifiedVariant {
            descriptor: descriptor.clone(),
            manifest,
            config,
            lock,
            target,
        });
    }
    Ok(verified)
}

fn unique_layer<'a>(
    manifest: &'a OciImageManifest,
    media_type: &str,
) -> Result<&'a Descriptor, BundleError> {
    let mut layers = manifest
        .layers
        .iter()
        .filter(|layer| layer.media_type == media_type);
    let layer = layers
        .next()
        .ok_or_else(|| BundleError::MissingEntry(media_type.to_owned()))?;
    if layers.next().is_some() {
        return Err(BundleError::ConflictingDescriptor(media_type.to_owned()));
    }
    Ok(layer)
}

fn require_bundle_version(label: &str, version: &str) -> Result<(), BundleError> {
    if version != BUNDLE_SCHEMA_VERSION {
        return Err(BundleError::UnsupportedSchema(format!(
            "{label} version {version}"
        )));
    }
    Ok(())
}

fn validate_layout(index: &OciIndex, blobs: &BTreeMap<String, Blob>) -> Result<(), BundleError> {
    validate_index_header(index)?;
    let mut referenced = BTreeSet::new();
    let mut descriptors = BTreeMap::new();
    for variant in &index.manifests {
        if variant.platform.is_none() || variant.media_type != media_types::OCI_IMAGE_MANIFEST {
            return Err(BundleError::InvalidDescriptor(variant.digest.clone()));
        }
        record_descriptor(variant, &mut descriptors)?;
        let manifest_blob = validate_layout_blob(variant, blobs)?;
        referenced.insert(variant.digest.clone());
        let manifest: OciImageManifest = serde_json::from_slice(&manifest_blob.bytes)?;
        if manifest.schema_version != 2 || manifest.media_type != media_types::OCI_IMAGE_MANIFEST {
            return Err(BundleError::UnsupportedSchema(variant.digest.clone()));
        }
        for descriptor in std::iter::once(&manifest.config).chain(manifest.layers.iter()) {
            record_descriptor(descriptor, &mut descriptors)?;
            validate_layout_blob(descriptor, blobs)?;
            referenced.insert(descriptor.digest.clone());
        }
    }
    if let Some(unreferenced) = blobs.keys().find(|digest| !referenced.contains(*digest)) {
        return Err(BundleError::UnreferencedBlob(unreferenced.clone()));
    }
    Ok(())
}

fn validate_index_header(index: &OciIndex) -> Result<(), BundleError> {
    if index.schema_version != 2 || index.media_type != media_types::OCI_IMAGE_INDEX {
        return Err(BundleError::UnsupportedSchema(format!(
            "index schemaVersion={} mediaType={}",
            index.schema_version, index.media_type
        )));
    }
    if index.manifests.is_empty() {
        return Err(BundleError::InvalidDescriptor(
            "index has no variants".to_owned(),
        ));
    }
    Ok(())
}

fn validate_layout_blob<'a>(
    descriptor: &Descriptor,
    blobs: &'a BTreeMap<String, Blob>,
) -> Result<&'a Blob, BundleError> {
    validate_descriptor_fields(descriptor)?;
    let blob = blobs
        .get(&descriptor.digest)
        .ok_or_else(|| BundleError::MissingEntry(descriptor.digest.clone()))?;
    if blob.bytes.len() as u64 != descriptor.size || sha256_digest(&blob.bytes) != descriptor.digest
    {
        return Err(BundleError::DigestMismatch(descriptor.digest.clone()));
    }
    if blob.media_type != descriptor.media_type {
        return Err(BundleError::ConflictingDescriptor(
            descriptor.digest.clone(),
        ));
    }
    Ok(blob)
}

fn verify_descriptor_blob<'a>(
    descriptor: &Descriptor,
    blobs: &'a BTreeMap<String, Vec<u8>>,
) -> Result<&'a [u8], BundleError> {
    validate_descriptor_fields(descriptor)?;
    let bytes = blobs
        .get(&descriptor.digest)
        .ok_or_else(|| BundleError::MissingEntry(descriptor.digest.clone()))?;
    if bytes.len() as u64 != descriptor.size || sha256_digest(bytes) != descriptor.digest {
        return Err(BundleError::DigestMismatch(descriptor.digest.clone()));
    }
    Ok(bytes)
}

fn validate_descriptor_fields(descriptor: &Descriptor) -> Result<(), BundleError> {
    if descriptor.media_type.is_empty() || !validate_digest(&descriptor.digest) {
        return Err(BundleError::InvalidDescriptor(descriptor.digest.clone()));
    }
    Ok(())
}

fn record_descriptor(
    descriptor: &Descriptor,
    descriptors: &mut BTreeMap<String, (String, u64)>,
) -> Result<(), BundleError> {
    validate_descriptor_fields(descriptor)?;
    let identity = (descriptor.media_type.clone(), descriptor.size);
    if let Some(previous) = descriptors.insert(descriptor.digest.clone(), identity.clone()) {
        if previous != identity {
            return Err(BundleError::ConflictingDescriptor(
                descriptor.digest.clone(),
            ));
        }
    }
    Ok(())
}

fn require_canonical_json<T: DeserializeOwned + Serialize>(
    name: &str,
    bytes: &[u8],
) -> Result<(), BundleError> {
    let value: T = serde_json::from_slice(bytes)?;
    if canonical_json(&value)? != bytes {
        return Err(BundleError::NonCanonicalJson(name.to_owned()));
    }
    Ok(())
}

fn require_canonical_value(name: &str, bytes: &[u8], limit: u64) -> Result<(), BundleError> {
    if bytes.len() as u64 > limit {
        return Err(BundleError::LimitExceeded(format!(
            "JSON metadata {name} exceeds {limit} bytes"
        )));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if canonical_json(&value)? != bytes {
        return Err(BundleError::NonCanonicalJson(name.to_owned()));
    }
    Ok(())
}

#[allow(dead_code)]
fn _schema_version_marker() -> &'static str {
    BUNDLE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write as _;

    use super::*;
    use crate::oci::{OciImageManifest, OciPlatform};
    use crate::schema::{
        AcceleratorBackend, BundleConfig, BundleLock, CompatibilityRange, PackerIdentity,
        TargetRequirements,
    };

    fn platform(architecture: &str) -> OciPlatform {
        OciPlatform {
            architecture: architecture.to_owned(),
            os: "linux".to_owned(),
            variant: None,
            os_version: None,
            os_features: Vec::new(),
        }
    }

    fn sample_layout(platforms: &[&str]) -> BundleLayout {
        let mut layout = BundleLayout::new();
        for architecture in platforms {
            let pipeline = layout
                .add_json_blob(
                    media_types::PIPELINE_MANIFEST,
                    &serde_json::json!({"connections": [], "nodes": [], "version": "v1"}),
                )
                .unwrap();
            let target_value = TargetRequirements {
                schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
                target_id: format!("linux-{architecture}"),
                os: "linux".to_owned(),
                architecture: architecture.to_string(),
                native_abi: None,
                manifest_schemas: vec!["v1".to_owned()],
                plugin_abi: CompatibilityRange {
                    minimum: "0.4.0".to_owned(),
                    maximum_exclusive: None,
                },
                python: None,
                accelerator: AcceleratorBackend::Cpu,
                minimum_memory_bytes: 0,
                minimum_disk_bytes: 0,
                media_devices: Vec::new(),
                runtime_features: Vec::new(),
            };
            let target = layout
                .add_json_blob(media_types::TARGET_REQUIREMENTS, &target_value)
                .unwrap();
            let identity = PackerIdentity {
                name: "test-packer".to_owned(),
                version: "1.0.0".to_owned(),
            };
            let lock_value = BundleLock {
                schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
                target_id: target_value.target_id,
                manifest_schema: "v1".to_owned(),
                manifest_digest: pipeline.digest.clone(),
                packer: identity.clone(),
                resolution_inputs_digest: format!("sha256:{}", "0".repeat(64)),
                runtime_compatibility: CompatibilityRange {
                    minimum: "0.4.0".to_owned(),
                    maximum_exclusive: None,
                },
                plugin_abi: CompatibilityRange {
                    minimum: "0.4.0".to_owned(),
                    maximum_exclusive: None,
                },
                plugins: Vec::new(),
                native_runtime: None,
                python: None,
                assets: Vec::new(),
            };
            let lock = layout
                .add_json_blob(media_types::LOCKFILE, &lock_value)
                .unwrap();
            let config = BundleConfig {
                schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
                created_by: identity,
                manifest_digest: pipeline.digest.clone(),
                lock_digest: lock.digest.clone(),
                target_digest: target.digest.clone(),
            };
            let config = layout
                .add_json_blob(media_types::BUNDLE_CONFIG, &config)
                .unwrap();
            layout
                .add_variant(
                    &OciImageManifest {
                        schema_version: 2,
                        media_type: media_types::OCI_IMAGE_MANIFEST.to_owned(),
                        config,
                        layers: vec![pipeline, lock, target],
                        annotations: BTreeMap::new(),
                    },
                    platform(architecture),
                )
                .unwrap();
        }
        layout
    }

    fn write(layout: &BundleLayout) -> Vec<u8> {
        let mut bytes = Vec::new();
        layout.write_rmpkg(&mut bytes).unwrap();
        bytes
    }

    fn raw_rmpkg(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut tar_bytes = Vec::new();
        for (path, data) in entries {
            let mut header = [0_u8; 512];
            assert!(path.len() <= 100);
            header[..path.len()].copy_from_slice(path.as_bytes());
            write_octal(&mut header[100..108], 0o644);
            write_octal(&mut header[108..116], 0);
            write_octal(&mut header[116..124], 0);
            write_octal(&mut header[124..136], data.len() as u64);
            write_octal(&mut header[136..148], 0);
            header[148..156].fill(b' ');
            header[156] = b'0';
            header[257..263].copy_from_slice(b"ustar\0");
            header[263..265].copy_from_slice(b"00");
            let checksum: u64 = header.iter().map(|byte| *byte as u64).sum();
            let checksum_text = format!("{checksum:06o}\0 ");
            header[148..156].copy_from_slice(checksum_text.as_bytes());
            tar_bytes.extend_from_slice(&header);
            tar_bytes.extend_from_slice(data);
            let padding = (512 - (data.len() % 512)) % 512;
            tar_bytes.resize(tar_bytes.len() + padding, 0);
        }
        tar_bytes.resize(tar_bytes.len() + 1024, 0);
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 19).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn write_octal(field: &mut [u8], value: u64) {
        field.fill(b'0');
        let text = format!("{value:o}");
        let start = field.len() - text.len() - 1;
        field[start..start + text.len()].copy_from_slice(text.as_bytes());
        field[field.len() - 1] = 0;
    }

    fn unpack(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
        let decoder = zstd::stream::read::Decoder::new(bytes).unwrap();
        tar::Archive::new(decoder)
            .entries()
            .unwrap()
            .map(|entry| {
                let mut entry = entry.unwrap();
                let path = entry.path().unwrap().to_string_lossy().into_owned();
                let mut data = Vec::new();
                entry.read_to_end(&mut data).unwrap();
                (path, data)
            })
            .collect()
    }

    #[test]
    fn repeated_writes_are_byte_identical() {
        let layout = sample_layout(&["amd64"]);
        let first = write(&layout);
        let second = write(&layout);
        assert_eq!(first, second);
        let verified =
            VerifiedBundle::read_rmpkg(first.as_slice(), BundleLimits::default()).unwrap();
        assert_eq!(verified.index.manifests.len(), 1);
    }

    #[test]
    fn multi_target_index_round_trips() {
        let bytes = write(&sample_layout(&["amd64", "arm64"]));
        let verified =
            VerifiedBundle::read_rmpkg(bytes.as_slice(), BundleLimits::default()).unwrap();
        let architectures: Vec<_> = verified
            .index
            .manifests
            .iter()
            .map(|descriptor| descriptor.platform.as_ref().unwrap().architecture.as_str())
            .collect();
        assert_eq!(architectures, ["amd64", "arm64"]);
    }

    #[test]
    fn traversal_path_is_rejected_before_json_parsing() {
        let bytes = raw_rmpkg(&[("../outside", b"payload")]);
        let error =
            VerifiedBundle::read_rmpkg(bytes.as_slice(), BundleLimits::default()).unwrap_err();
        assert!(matches!(error, BundleError::UnsafePath(_)));
    }

    #[test]
    fn duplicate_archive_path_is_rejected() {
        let bytes = raw_rmpkg(&[(OCI_LAYOUT_PATH, b"{}"), (OCI_LAYOUT_PATH, b"{}")]);
        let error =
            VerifiedBundle::read_rmpkg(bytes.as_slice(), BundleLimits::default()).unwrap_err();
        assert!(matches!(error, BundleError::DuplicatePath(_)));
    }

    #[test]
    fn digest_mismatch_is_rejected() {
        let valid = write(&sample_layout(&["amd64"]));
        let mut entries = unpack(&valid);
        let (_, data) = entries
            .iter_mut()
            .find(|(path, _)| path.starts_with(BLOBS_PREFIX))
            .unwrap();
        data[0] ^= 1;
        let borrowed: Vec<_> = entries
            .iter()
            .map(|(path, data)| (path.as_str(), data.as_slice()))
            .collect();
        let corrupt = raw_rmpkg(&borrowed);
        let error =
            VerifiedBundle::read_rmpkg(corrupt.as_slice(), BundleLimits::default()).unwrap_err();
        assert!(matches!(error, BundleError::DigestMismatch(_)));
    }

    #[test]
    fn oversized_metadata_is_rejected() {
        let bytes = write(&sample_layout(&["amd64"]));
        let limits = BundleLimits {
            max_metadata_bytes: 4,
            ..BundleLimits::default()
        };
        let error = VerifiedBundle::read_rmpkg(bytes.as_slice(), limits).unwrap_err();
        assert!(matches!(error, BundleError::LimitExceeded(_)));
    }

    #[test]
    fn unsupported_layout_version_is_rejected() {
        let layout = canonical_json(&serde_json::json!({"imageLayoutVersion": "9.0.0"})).unwrap();
        let index = canonical_json(&OciIndex::default()).unwrap();
        let bytes = raw_rmpkg(&[(OCI_LAYOUT_PATH, &layout), (INDEX_PATH, &index)]);
        let error =
            VerifiedBundle::read_rmpkg(bytes.as_slice(), BundleLimits::default()).unwrap_err();
        assert!(matches!(error, BundleError::UnsupportedLayout(_)));
    }

    #[test]
    fn conflicting_descriptor_identity_is_rejected() {
        let mut layout = sample_layout(&["amd64"]);
        let variant = layout.index.manifests[0].clone();
        let blob = layout.blobs.get(&variant.digest).unwrap();
        let mut manifest: OciImageManifest = serde_json::from_slice(&blob.bytes).unwrap();
        let mut conflict = manifest.config.clone();
        conflict.media_type = "application/vnd.example.conflict".to_owned();
        manifest.layers.push(conflict);
        let replacement = canonical_json(&manifest).unwrap();
        let replacement_digest = sha256_digest(&replacement);
        layout.blobs.remove(&variant.digest);
        layout.blobs.insert(
            replacement_digest.clone(),
            Blob {
                media_type: media_types::OCI_IMAGE_MANIFEST.to_owned(),
                bytes: replacement.clone(),
            },
        );
        layout.index.manifests[0].digest = replacement_digest;
        layout.index.manifests[0].size = replacement.len() as u64;
        let error = layout.write_rmpkg(Vec::new()).unwrap_err();
        assert!(matches!(error, BundleError::ConflictingDescriptor(_)));
    }
}
