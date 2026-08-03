use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::archive::BundleError;

/// Serialize a value using RFC 8785 JSON Canonicalization Scheme semantics.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, BundleError> {
    serde_json_canonicalizer::to_vec(value).map_err(BundleError::Json)
}

/// Return an OCI-compatible SHA-256 digest string.
pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

pub(crate) fn validate_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
