//! Audio processing nodes
//!
//! High-performance Rust implementations of audio nodes using fast AudioData path

use crate::executor::node_executor::{NodeContext, NodeExecutor};
use crate::nodes::registry::NodeRegistry;
use crate::Result;
use serde_json::Value;
use std::sync::Arc;

pub mod fast;
pub mod format_converter_fast;
pub mod resample_fast;
pub mod vad_fast;

// Old JSON-based nodes (archived - kept for reference)
// These have been moved to archive/old-audio-nodes-json/
// pub mod format_converter;
// pub mod resample;
// pub mod vad;

pub use fast::FastAudioNode;
pub use format_converter_fast::FastFormatConverter;
pub use resample_fast::{FastResampleNode, ResampleQuality};
pub use vad_fast::FastVADNode;

/// Create a registry with all audio processing nodes registered
///
/// This registers high-performance Rust-native implementations using the FastAudioNode trait:
/// - FastResampleNode: Zero-copy audio resampling (no JSON overhead)
/// - FastVADNode: Voice Activity Detection using energy analysis
/// - FastFormatConverter: Audio format conversion (F32 ↔ I16 ↔ I32)
///
/// These nodes are 10-15x faster than the old JSON-based nodes.
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::audio::create_audio_registry;
///
/// let audio_registry = create_audio_registry();
/// // Use in CompositeRegistry or standalone
/// ```
pub fn create_audio_registry() -> NodeRegistry {
    use crate::nodes::registry::NodeFactory;

    let mut registry = NodeRegistry::new();

    // Register fast audio nodes with stub factories
    // These nodes are actually created dynamically in execute_fast_pipeline
    // but need to be registered here for version discovery

    struct StubFactory {
        node_type: &'static str,
    }

    impl NodeFactory for StubFactory {
        fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
            Ok(Box::new(StubFastNode {
                node_type: self.node_type,
            }))
        }

        fn node_type(&self) -> &str {
            self.node_type
        }

        fn is_rust_native(&self) -> bool {
            true
        }
    }

    registry.register_rust(Arc::new(StubFactory {
        node_type: "RustResampleNode",
    }));
    registry.register_rust(Arc::new(StubFactory {
        node_type: "RustVADNode",
    }));
    registry.register_rust(Arc::new(StubFactory {
        node_type: "RustFormatConverterNode",
    }));

    registry
}

/// Stub node for fast node registration (not actually used for execution)
struct StubFastNode {
    node_type: &'static str,
}

#[async_trait::async_trait]
impl NodeExecutor for StubFastNode {
    async fn initialize(&mut self, _ctx: &NodeContext) -> Result<()> {
        Ok(())
    }

    async fn process(&mut self, _input: Value) -> Result<Vec<Value>> {
        Err(crate::Error::Execution(format!(
            "{} should be executed via fast pipeline path",
            self.node_type
        )))
    }

    async fn cleanup(&mut self) -> Result<()> {
        Ok(())
    }

    fn is_streaming(&self) -> bool {
        false
    }

    async fn finish_streaming(&mut self) -> Result<Vec<Value>> {
        Ok(vec![])
    }
}
