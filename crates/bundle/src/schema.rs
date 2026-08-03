use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BundleConfig {
    pub schema_version: String,
    pub created_by: PackerIdentity,
    pub manifest_digest: String,
    pub lock_digest: String,
    pub target_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PackerIdentity {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BundleLock {
    pub schema_version: String,
    pub target_id: String,
    pub manifest_schema: String,
    pub manifest_digest: String,
    pub packer: PackerIdentity,
    pub resolution_inputs_digest: String,
    pub runtime_compatibility: CompatibilityRange,
    pub plugin_abi: CompatibilityRange,
    #[serde(default)]
    pub plugins: Vec<LockedPlugin>,
    #[serde(default)]
    pub python: Option<LockedPythonEnvironment>,
    #[serde(default)]
    pub assets: Vec<AssetDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BundleLockSet {
    pub schema_version: String,
    pub variants: BTreeMap<String, BundleLock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompatibilityRange {
    pub minimum: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_exclusive: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LockedPlugin {
    pub name: String,
    pub version: String,
    pub source: ImmutableSource,
    pub artifact_digest: String,
    pub target: String,
    pub plugin_abi: String,
    pub node_types: Vec<String>,
    pub kind: PluginKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    Native,
    PythonWheel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LockedPythonEnvironment {
    pub implementation: String,
    pub version: String,
    pub abi: String,
    pub accelerator: AcceleratorBackend,
    pub wheel_set_digest: String,
    pub wheels: Vec<LockedWheel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LockedWheel {
    pub name: String,
    pub version: String,
    pub filename: String,
    pub digest: String,
    pub size: u64,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TargetRequirements {
    pub schema_version: String,
    pub target_id: String,
    pub os: String,
    pub architecture: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_abi: Option<String>,
    pub manifest_schemas: Vec<String>,
    pub plugin_abi: CompatibilityRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python: Option<PythonTarget>,
    pub accelerator: AcceleratorBackend,
    pub minimum_memory_bytes: u64,
    pub minimum_disk_bytes: u64,
    #[serde(default)]
    pub media_devices: Vec<String>,
    #[serde(default)]
    pub runtime_features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PythonTarget {
    pub implementation: String,
    pub version: String,
    pub abi: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AcceleratorBackend {
    Cpu,
    Cuda { version: String },
    Rocm { version: String },
    Metal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetDescriptor {
    pub name: String,
    pub digest: String,
    pub size: u64,
    pub cache_key: String,
    pub license: Option<String>,
    #[serde(flatten)]
    pub storage: AssetStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "storage", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssetStorage {
    Embedded,
    External {
        source: String,
        revision: String,
        credentials: CredentialPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialPolicy {
    Forbidden,
    TargetMaySupply,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImmutableSource {
    pub kind: String,
    pub location: String,
    pub revision: String,
    pub source_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Provenance {
    pub schema_version: String,
    pub builder: PackerIdentity,
    pub invocation_digest: String,
    pub materials: Vec<ProvenanceMaterial>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProvenanceMaterial {
    pub uri: String,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VerificationLevel {
    Inspect,
    Structural,
    Install,
    Smoke,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrustPolicy {
    LocalDevelopment,
    RequirePublisherSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationReport {
    pub schema_version: String,
    pub level: VerificationLevel,
    pub success: bool,
    pub bundle_digest: String,
    #[serde(default)]
    pub findings: Vec<VerificationFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationFinding {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeCapabilities {
    pub schema_version: String,
    pub os: String,
    pub architecture: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_abi: Option<String>,
    pub manifest_schemas: Vec<String>,
    pub plugin_abi: CompatibilityRange,
    #[serde(default)]
    pub python: Vec<PythonTarget>,
    pub accelerators: Vec<AcceleratorBackend>,
    pub memory_bytes: u64,
    pub available_cache_bytes: u64,
    #[serde(default)]
    pub media_devices: Vec<String>,
    #[serde(default)]
    pub runtime_features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VariantCandidate {
    pub descriptor: DescriptorIdentity,
    pub requirements: TargetRequirements,
    #[serde(default)]
    pub required_blobs: Vec<DescriptorIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DescriptorIdentity {
    pub digest: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreflightReport {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<DescriptorIdentity>,
    pub cached_bytes: u64,
    pub missing_bytes: u64,
    pub additional_required_bytes: u64,
    #[serde(default)]
    pub missing_blobs: Vec<DescriptorIdentity>,
    #[serde(default)]
    pub rejections: Vec<VariantRejection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VariantRejection {
    pub descriptor_digest: String,
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProvisioningPhase {
    Resolving,
    Transferring,
    Verifying,
    InstallingPython,
    FetchingAssets,
    Loading,
    Warming,
    SmokeTesting,
    Ready,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallStatus {
    pub operation_id: String,
    pub phase: ProvisioningPhase,
    pub completed_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}
