//! Validation error types
//!
//! Re-exported from `remotemedia-types` so the host's richer `Error`
//! variant (`Error::Validation(Vec<ValidationError>)`) and plugin code
//! share the same struct definition.

pub use remotemedia_types::{
    ValidationConstraint, ValidationError, ValidationWarning, WarningType,
};
