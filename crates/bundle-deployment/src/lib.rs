//! Transport-neutral, filesystem-backed bundle installation primitives.
//!
//! Network services authenticate callers before invoking this crate. The crate
//! never accepts shell commands or author-controlled target paths.

mod activation;
mod preflight;
mod service;
mod store;

pub use activation::{ActivationRegistry, DeploymentInfo, DeploymentRevision};
pub use preflight::preflight;
pub use service::{DeploymentService, TokenAuthenticator};
pub use store::{
    ContentStore, ExternalAssetTransport, ReqwestExternalAssetTransport, UploadSession,
};

#[derive(Debug, thiserror::Error)]
pub enum DeploymentError {
    #[error("deployment I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("deployment state is invalid: {0}")]
    State(#[from] serde_json::Error),
    #[error("bundle metadata is invalid: {0}")]
    Bundle(#[from] remotemedia_bundle::BundleError),
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("external asset source is invalid: {0}")]
    InvalidAssetSource(String),
    #[error("external asset fetch failed: {0}")]
    ExternalAssetFetch(String),
    #[error("content digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("content size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("upload offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch { expected: u64, actual: u64 },
    #[error("deployment name is invalid: {0}")]
    InvalidName(String),
    #[error("native runtime path is invalid: {0}")]
    InvalidRuntimePath(String),
    #[error("deployment has no previous revision: {0}")]
    NoPreviousRevision(String),
    #[error("deployment revision is not installed: {0}")]
    NotInstalled(String),
    #[error("deployment revision has no manifest digest: {0}")]
    MissingManifestDigest(String),
    #[error("deployment request is not authenticated")]
    Unauthenticated,
    #[error("deployment operation already exists: {0}")]
    OperationExists(String),
    #[error("deployment operation was cancelled: {0}")]
    Cancelled(String),
    #[error("deployment provisioning failed: {0}")]
    Provisioning(String),
}
