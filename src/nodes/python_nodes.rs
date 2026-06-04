//! Python Node Factories
//!
//! This module provides factory implementations for Python SDK nodes
//! that are executed via CPython (PyO3).

use crate::executor::node_executor::{NodeExecutor, PythonNodeExecutor};
use crate::nodes::registry::{NodeFactory, NodeRegistry};
use crate::Result;
use serde_json::Value;
use std::sync::Arc;

/// Factory for KokoroTTSNode (Python TTS engine)
pub struct KokoroTTSNodeFactory;

impl NodeFactory for KokoroTTSNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        // Use PythonNodeExecutor wrapper which properly implements NodeExecutor trait
        let executor = PythonNodeExecutor::new("KokoroTTSNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "KokoroTTSNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Factory for VibeVoiceTTSNode (Python TTS engine)
pub struct VibeVoiceTTSNodeFactory;

impl NodeFactory for VibeVoiceTTSNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        // Use PythonNodeExecutor wrapper which properly implements NodeExecutor trait
        let executor = PythonNodeExecutor::new("VibeVoiceTTSNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "VibeVoiceTTSNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Factory for SimplePyTorchNode (minimal PyTorch test)
pub struct SimplePyTorchNodeFactory;

impl NodeFactory for SimplePyTorchNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        let executor = PythonNodeExecutor::new("SimplePyTorchNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "SimplePyTorchNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Factory for CosyVoice3TTSNode (Python TTS engine)
pub struct CosyVoice3TTSNodeFactory;

impl NodeFactory for CosyVoice3TTSNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        let executor = PythonNodeExecutor::new("CosyVoice3TTSNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "CosyVoice3TTSNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Factory for VoxtralTTSNode (Python TTS engine)
pub struct VoxtralTTSNodeFactory;

impl NodeFactory for VoxtralTTSNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        let executor = PythonNodeExecutor::new("VoxtralTTSNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "VoxtralTTSNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Factory for LFM2AudioNode (Python LFM2 audio generation)
pub struct LFM2AudioNodeFactory;

impl NodeFactory for LFM2AudioNodeFactory {
    fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
        let executor = PythonNodeExecutor::new("LFM2AudioNode");
        Ok(Box::new(executor))
    }

    fn node_type(&self) -> &str {
        "LFM2AudioNode"
    }

    fn is_rust_native(&self) -> bool {
        false // Python implementation
    }
}

/// Create a registry with Python TTS nodes
///
/// This registers Python-based nodes that are available via the
/// remotemedia Python package (examples/audio_examples/kokoro_tts.py)
pub fn create_python_tts_registry() -> NodeRegistry {
    let mut registry = NodeRegistry::new();

    // Register KokoroTTSNode as Python implementation
    registry.register_python(Arc::new(KokoroTTSNodeFactory));

    // Register VibeVoiceTTSNode as Python implementation
    registry.register_python(Arc::new(VibeVoiceTTSNodeFactory));

    registry.register_python(Arc::new(CosyVoice3TTSNodeFactory));

    registry.register_python(Arc::new(VoxtralTTSNodeFactory));

    registry.register_python(Arc::new(LFM2AudioNodeFactory));

    // Register SimplePyTorchNode for testing
    registry.register_python(Arc::new(SimplePyTorchNodeFactory));

    registry
}
