//! Media types used by RemoteMedia OCI descriptors.

pub const OCI_IMAGE_INDEX: &str = "application/vnd.oci.image.index.v1+json";
pub const OCI_IMAGE_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
pub const OCI_IMAGE_CONFIG: &str = "application/vnd.oci.image.config.v1+json";

pub const BUNDLE_CONFIG: &str = "application/vnd.remotemedia.pipeline.bundle.config.v1+json";
pub const PIPELINE_MANIFEST: &str = "application/vnd.remotemedia.pipeline.manifest.v1+json";
pub const LOCKFILE: &str = "application/vnd.remotemedia.pipeline.lock.v1+json";
pub const TARGET_REQUIREMENTS: &str =
    "application/vnd.remotemedia.pipeline.target-requirements.v1+json";
pub const NATIVE_PLUGIN: &str = "application/vnd.remotemedia.pipeline.plugin.native.v1";
pub const NATIVE_RUNTIME_FILE: &str = "application/vnd.remotemedia.pipeline.runtime.native.v1";
pub const PYTHON_WHEEL: &str = "application/vnd.remotemedia.pipeline.python.wheel.v1";
pub const EMBEDDED_ASSET: &str = "application/vnd.remotemedia.pipeline.asset.v1";
pub const EXTERNAL_ASSETS: &str = "application/vnd.remotemedia.pipeline.assets.v1+json";
pub const SBOM_SPDX: &str = "application/spdx+json";
pub const SBOM_CYCLONEDX: &str = "application/vnd.cyclonedx+json";
pub const PROVENANCE: &str = "application/vnd.in-toto+json";
pub const SIGNATURE: &str = "application/vnd.dev.sigstore.bundle+json";
pub const SMOKE_FIXTURES: &str = "application/vnd.remotemedia.pipeline.smoke.v1+json";

pub fn is_json(media_type: &str) -> bool {
    media_type.ends_with("+json") || media_type == "application/json"
}
