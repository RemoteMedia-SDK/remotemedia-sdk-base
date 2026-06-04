//! Media Capabilities Negotiation Module (spec 022)
//!
//! This module provides GStreamer-style media capabilities negotiation for pipeline nodes.
//! Nodes can declare their input requirements and output capabilities, and the system
//! validates compatibility during pipeline construction.
//!
//! # Features
//!
//! - **Capability Declaration**: Nodes declare input requirements and output capabilities
//! - **Pipeline Validation**: Automatic validation of connected nodes' compatibility
//! - **Auto-Conversion**: Optional automatic insertion of conversion nodes
//! - **Flexible Negotiation**: Nodes with flexible constraints adapt to their connections
//! - **Introspection**: Query resolved capabilities at each connection point
//!
//! # Example
//!
//! ```rust,ignore
//! use remotemedia_core::capabilities::{
//!     MediaCapabilities, MediaConstraints, AudioConstraints, ConstraintValue,
//! };
//!
//! // Create a node that requires 16kHz mono f32 audio input
//! let input_caps = MediaCapabilities::with_input(
//!     MediaConstraints::Audio(AudioConstraints {
//!         sample_rate: Some(ConstraintValue::Exact(16000)),
//!         channels: Some(ConstraintValue::Exact(1)),
//!         format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
//!     })
//! );
//!
//! // Create a flexible node that accepts any sample rate in a range
//! let flexible_caps = MediaCapabilities::with_input(
//!     MediaConstraints::Audio(AudioConstraints {
//!         sample_rate: Some(ConstraintValue::Range { min: 8000, max: 48000 }),
//!         channels: None, // Any channel count
//!         format: None,   // Any format
//!     })
//! );
//! ```

// Core constraint types (FR-003 through FR-008)
pub mod constraints;

// Validation logic (FR-009 through FR-012)
pub mod validation;

// Negotiation algorithm (FR-013 through FR-015)
pub mod negotiation;

// Conversion registry (FR-014)
pub mod registry;

// Dynamic capability resolution (runtime-dependent capabilities)
pub mod dynamic;

// Pipeline capability resolver (spec 023)
pub mod resolver;

// Manifest-level negotiation entry point (ties registry + negotiation together)
pub mod manifest_negotiation;

// Re-export main types for convenience
pub use constraints::{
    AudioConstraints, AudioSampleFormat, ConstraintValue, FileConstraints, JsonConstraints,
    MediaCapabilities, MediaConstraints, PixelFormat, TensorConstraints, TensorDataType,
    TextConstraints, VideoConstraints,
};

pub use validation::{CapabilityMismatch, CapabilityValidationResult};

pub use negotiation::{
    ConversionPath, ConversionStep, InsertedNode, NegotiatedCapabilities, NegotiationResult,
};

pub use registry::{ConversionRegistry, ConverterInfo, DefaultConversionRegistry};

pub use dynamic::{
    CapabilityBehavior, CapabilityNotification, CapabilitySource, DynamicCapabilityProvider,
    ResolutionContext, ResolutionState, ResolvedCapabilities,
};

pub use resolver::{CapabilityHints, CapabilityResolver, NodeHints};

pub use manifest_negotiation::{
    negotiate_manifest, strict_capabilities_enabled, NegotiationOutcome,
};
