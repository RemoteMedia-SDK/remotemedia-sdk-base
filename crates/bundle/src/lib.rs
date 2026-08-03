//! Versioned contracts and deterministic OCI archives for RemoteMedia pipeline bundles.
//!
//! This crate deliberately contains no pipeline execution code. Both packers and
//! deployment services can inspect and structurally verify an `.rmpkg` without
//! loading plugins, importing Python, or executing bundle content.

pub mod archive;
pub mod canonical;
pub mod media_types;
pub mod oci;
pub mod schema;

pub use archive::{BundleError, BundleLayout, BundleLimits, VerifiedBundle};
pub use canonical::{canonical_json, sha256_digest};
pub use oci::{Descriptor, OciImageLayout, OciImageManifest, OciIndex, OciPlatform};
pub use schema::*;

/// Bundle contract version emitted by this implementation.
pub const BUNDLE_SCHEMA_VERSION: &str = "1";
/// OCI Image Layout version supported by `.rmpkg` readers and writers.
pub const OCI_LAYOUT_VERSION: &str = "1.0.0";
