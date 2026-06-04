//! Dead-code stash of `StreamingNodeFactory` implementations that wrap
//! `PythonStreamingNode`.
//!
//! These factory definitions were authored against an earlier
//! registration scheme that no longer drives any callers — the
//! actually-registered Python factories live in
//! `crate::nodes::python_nodes` and implement `NodeFactory` (a
//! different trait shape).
//!
//! They are kept here (gated as a single sub-module) in case future
//! work needs `StreamingNodeFactory`-shaped Python wrappers. The whole
//! module is built only with `feature = "multiprocess"` because every
//! factory body constructs `PythonStreamingNode`, which itself is
//! `multiprocess`-only.
//!
//! If these grow unused for too long, delete them — `python_nodes.rs`
//! has the live counterparts.

use serde_json::Value;
use std::sync::Arc;

use crate::nodes::python_streaming::PythonStreamingNode;
use crate::nodes::{AsyncNodeWrapper, StreamingNode, StreamingNodeFactory};
use crate::Error;

/// WhisperX transcription node (Python) - provides word-level timestamps via alignment
struct WhisperXNodeFactory;
impl StreamingNodeFactory for WhisperXNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "WhisperXTranscriber", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "WhisperXTranscriber", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "WhisperXNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("WhisperXNode")
                .description("Speech-to-text with word-level timestamps using WhisperX (Python)")
                .category("stt")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json]),
        )
    }
}

/// HuggingFace Whisper transcription node (Python) - word-level timestamps via transformers
struct HFWhisperNodeFactory;
impl StreamingNodeFactory for HFWhisperNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "WhisperTranscriptionNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "WhisperTranscriptionNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "HFWhisperNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Yields WordUpdate objects for each word
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("HFWhisperNode")
                .description(
                    "Speech-to-text with word-level timestamps using HuggingFace Whisper (Python)",
                )
                .category("stt")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json]),
        )
    }
}

struct KokoroTTSNodeFactory;
impl StreamingNodeFactory for KokoroTTSNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "KokoroTTSNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "KokoroTTSNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "KokoroTTSNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Kokoro yields multiple audio chunks per text input
    }
}

struct VibeVoiceTTSNodeFactory;
impl StreamingNodeFactory for VibeVoiceTTSNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "VibeVoiceTTSNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "VibeVoiceTTSNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VibeVoiceTTSNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // VibeVoice yields multiple audio chunks per text input
    }
}

struct CosyVoice3TTSNodeFactory;
impl StreamingNodeFactory for CosyVoice3TTSNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "CosyVoice3TTSNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "CosyVoice3TTSNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "CosyVoice3TTSNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // CosyVoice3 yields multiple audio chunks per text input
    }
}

struct VoxtralTTSNodeFactory;
impl StreamingNodeFactory for VoxtralTTSNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "VoxtralTTSNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "VoxtralTTSNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VoxtralTTSNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct SimplePyTorchNodeFactory;
impl StreamingNodeFactory for SimplePyTorchNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "SimplePyTorchNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "SimplePyTorchNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "SimplePyTorchNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }
}

struct LFM2AudioNodeFactory;
impl StreamingNodeFactory for LFM2AudioNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "LFM2AudioNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "LFM2AudioNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "LFM2AudioNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // LFM2Audio yields multiple tokens (text and audio) per input
    }
}

// Test node factories for Python streaming nodes
struct ExpanderNodeFactory;
impl StreamingNodeFactory for ExpanderNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "ExpanderNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "ExpanderNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "ExpanderNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct RangeGeneratorNodeFactory;
impl StreamingNodeFactory for RangeGeneratorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "RangeGeneratorNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "RangeGeneratorNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "RangeGeneratorNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct TransformAndExpandNodeFactory;
impl StreamingNodeFactory for TransformAndExpandNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "TransformAndExpandNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "TransformAndExpandNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "TransformAndExpandNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct ChainedTransformNodeFactory;
impl StreamingNodeFactory for ChainedTransformNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "ChainedTransformNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "ChainedTransformNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "ChainedTransformNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct ConditionalExpanderNodeFactory;
impl StreamingNodeFactory for ConditionalExpanderNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "ConditionalExpanderNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "ConditionalExpanderNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "ConditionalExpanderNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

struct FilterNodeFactory;
impl StreamingNodeFactory for FilterNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, "FilterNode", params, sid)?
        } else {
            PythonStreamingNode::new(node_id, "FilterNode", params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "FilterNode"
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // May output 0 or 1 items per input
    }
}
