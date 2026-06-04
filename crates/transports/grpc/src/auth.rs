//! Authentication middleware for gRPC service
//!
//! Implements API token validation via tower interceptor.
//! Tokens are passed in gRPC metadata as "authorization: Bearer <token>".

use std::collections::HashSet;
use std::sync::Arc;
use tonic::{Request, Status};

/// Authentication configuration
#[derive(Clone, Debug)]
pub struct AuthConfig {
    /// Set of valid API tokens
    pub valid_tokens: Arc<HashSet<String>>,
    /// Whether authentication is required (false for dev/testing)
    pub require_auth: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            valid_tokens: Arc::new(HashSet::new()),
            require_auth: false,
        }
    }
}

impl AuthConfig {
    /// Create new auth config with a set of valid tokens
    pub fn new(tokens: Vec<String>, require_auth: bool) -> Self {
        Self {
            valid_tokens: Arc::new(tokens.into_iter().collect()),
            require_auth,
        }
    }

    /// Check if a token is valid
    pub fn validate_token(&self, token: &str) -> bool {
        if !self.require_auth {
            return true;
        }
        self.valid_tokens.contains(token)
    }
}

/// Extract and validate bearer token from gRPC metadata
///
/// Expected format: "authorization: Bearer <token>"
pub fn check_auth<T>(request: &Request<T>, config: &AuthConfig) -> Result<(), Status> {
    // Skip auth if not required
    if !config.require_auth {
        return Ok(());
    }

    // Extract authorization header
    let metadata = request.metadata();
    let auth_header = metadata
        .get("authorization")
        .ok_or_else(|| {
            Status::unauthenticated("Missing authorization header. Include 'authorization: Bearer <token>' in gRPC metadata.")
        })?;

    // Parse bearer token
    let auth_str = auth_header
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid authorization header encoding"))?;

    let token = auth_str.strip_prefix("Bearer ").ok_or_else(|| {
        Status::unauthenticated("Invalid authorization format. Expected 'Bearer <token>'")
    })?;

    // Validate token
    if !config.validate_token(token) {
        return Err(Status::unauthenticated(
            "Invalid API token. Check your authentication credentials.",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn test_auth_config_validation() {
        let config = AuthConfig::new(
            vec!["test-token-123".to_string(), "prod-token-456".to_string()],
            true,
        );

        assert!(config.validate_token("test-token-123"));
        assert!(config.validate_token("prod-token-456"));
        assert!(!config.validate_token("invalid-token"));
    }

    #[test]
    fn test_auth_config_disabled() {
        let config = AuthConfig::new(vec![], false);
        assert!(config.validate_token("any-token")); // Should pass when disabled
    }

    #[test]
    fn test_check_auth_missing_header() {
        let config = AuthConfig::new(vec!["valid-token".to_string()], true);
        let request = Request::new(());

        let result = check_auth(&request, &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_check_auth_valid_token() {
        let config = AuthConfig::new(vec!["valid-token".to_string()], true);
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer valid-token"),
        );

        let result = check_auth(&request, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_auth_invalid_token() {
        let config = AuthConfig::new(vec!["valid-token".to_string()], true);
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer invalid-token"),
        );

        let result = check_auth(&request, &config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_check_auth_malformed_header() {
        let config = AuthConfig::new(vec!["valid-token".to_string()], true);
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("InvalidFormat valid-token"),
        );

        let result = check_auth(&request, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_auth_disabled() {
        let config = AuthConfig::new(vec![], false);
        let request = Request::new(()); // No auth header

        let result = check_auth(&request, &config);
        assert!(result.is_ok()); // Should pass when auth disabled
    }
}
