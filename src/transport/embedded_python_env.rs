//! Decoded, transport-agnostic representation of an embedded Python
//! environment shipped inline with a request, mirroring the `embedded_plugins`
//! `(digest, bytes)` tuple the `PipelineClient` trait already carries.
//!
//! The gRPC transport re-encodes this into the generated
//! `remotemedia.v1.EmbeddedPythonEnv` proto; WebRTC/HTTP stub transports
//! accept and ignore it.

use serde::{Deserialize, Serialize};

/// Locked interpreter target the wheels were frozen against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddedInterpreter {
    pub implementation: String,
    pub version: String,
    pub abi: String,
    pub accelerator: String,
}

/// A single frozen Python wheel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddedWheel {
    pub name: String,
    pub filename: String,
    pub digest: String,
    pub content: Vec<u8>,
}

/// A self-contained Python environment (frozen wheelhouse) shipped inline.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddedPythonEnv {
    pub interpreter: EmbeddedInterpreter,
    pub wheel_set_digest: String,
    pub wheels: Vec<EmbeddedWheel>,
}
