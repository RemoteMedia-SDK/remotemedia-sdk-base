//! Pipeline media-output scanner.
//!
//! Walks a [`Manifest`] and, for each node, asks the registered
//! [`StreamingNodeFactory`] what media outputs it produces and at which
//! `stream_id`. The result is consumed by [`crate::peer::ServerPeer`] to
//! pre-register one WebRTC track per declared output before SDP exchange,
//! so the answer naturally carries one m=audio / m=video section per
//! stream and no later renegotiation is needed.
//!
//! `stream_id` is extracted by convention from node params:
//!   - `video_stream_id` (string) for video outputs
//!   - `audio_stream_id` (string) for audio outputs
//! Falls back to `"default"` when the param is absent.

use remotemedia_core::capabilities::ConstraintValue;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::nodes::StreamingNodeRegistry;

/// Specification for one outbound audio stream.
#[derive(Debug, Clone, PartialEq)]
pub struct AudioStreamSpec {
    pub stream_id: String,
    pub sample_rate: u32,
    pub channels: u16,
}

impl AudioStreamSpec {
    /// Default 48 kHz mono spec for the named stream.
    pub fn default_named(stream_id: impl Into<String>) -> Self {
        Self {
            stream_id: stream_id.into(),
            sample_rate: 48_000,
            channels: 1,
        }
    }
}

/// Specification for one outbound video stream.
#[derive(Debug, Clone, PartialEq)]
pub struct VideoStreamSpec {
    pub stream_id: String,
    pub width: u32,
    pub height: u32,
    pub framerate: u32,
}

/// Pre-SDP plan describing every outbound media track this peer should expose.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MediaStreamPlan {
    pub audio_outputs: Vec<AudioStreamSpec>,
    pub video_outputs: Vec<VideoStreamSpec>,
}

/// Scan a manifest for outbound audio/video stream declarations.
///
/// The scanner is best-effort: nodes whose factory isn't registered (e.g.
/// `#[cfg]`-gated factories absent from the build) are skipped. The caller
/// is expected to apply its own back-compat fallback (typically: register a
/// single [`crate::media::DEFAULT_STREAM_ID`] audio track when
/// `audio_outputs` is empty).
pub fn scan(manifest: &Manifest, registry: &StreamingNodeRegistry) -> MediaStreamPlan {
    use remotemedia_core::capabilities::MediaConstraints;

    let mut plan = MediaStreamPlan::default();
    // De-duplication is global across the whole manifest. This catches
    // the common case (two nodes producing media with the same
    // stream_id — a configuration error). It also folds together a
    // less common case (one node declaring two outputs with the same
    // stream_id), which we treat the same way: keep the first, warn.
    let mut seen_audio: std::collections::HashSet<String> = Default::default();
    let mut seen_video: std::collections::HashSet<String> = Default::default();

    for node in &manifest.nodes {
        let Some(factory) = registry.get_factory(&node.node_type) else {
            tracing::debug!(
                node_id = %node.id,
                node_type = %node.node_type,
                "scanner: skipping node — factory not registered (likely feature-gated)"
            );
            continue;
        };

        // Primary path: ask the factory for declared capabilities.
        let caps = factory.media_capabilities(&node.params);

        if let Some(caps) = caps {
            for constraints in caps.outputs.values() {
                match constraints {
                    MediaConstraints::Video(vc) => {
                        let stream_id = node
                            .params
                            .get("video_stream_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string();
                        if !seen_video.insert(stream_id.clone()) {
                            tracing::warn!(
                                stream_id = %stream_id,
                                node_id = %node.id,
                                "scanner: duplicate video stream_id, keeping first"
                            );
                            continue;
                        }
                        let width = node
                            .params
                            .get("width")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .or_else(|| exact_u32(&vc.width))
                            .unwrap_or(1024);
                        let height = node
                            .params
                            .get("height")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .or_else(|| exact_u32(&vc.height))
                            .unwrap_or(1024);
                        let framerate = node
                            .params
                            .get("framerate")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .or_else(|| exact_f32_as_u32(&vc.framerate))
                            .unwrap_or(30);
                        plan.video_outputs.push(VideoStreamSpec {
                            stream_id,
                            width,
                            height,
                            framerate,
                        });
                    }
                    MediaConstraints::Audio(ac) => {
                        let stream_id = node
                            .params
                            .get("audio_stream_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string();
                        if !seen_audio.insert(stream_id.clone()) {
                            tracing::warn!(
                                stream_id = %stream_id,
                                node_id = %node.id,
                                "scanner: duplicate audio stream_id, keeping first"
                            );
                            continue;
                        }
                        let sample_rate = node
                            .params
                            .get("sample_rate")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .or_else(|| exact_u32(&ac.sample_rate))
                            .unwrap_or(48_000);
                        // AudioConstraints.channels is ConstraintValue<u32>;
                        // narrow to u16 because AudioEncoderConfig.channels is u16.
                        let channels = node
                            .params
                            .get("channels")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u16)
                            .or_else(|| exact_u32(&ac.channels).map(|v| v as u16))
                            .unwrap_or(1);
                        plan.audio_outputs.push(AudioStreamSpec {
                            stream_id,
                            sample_rate,
                            channels,
                        });
                    }
                    _ => {}
                }
            }
        } else {
            // Fallback: factory didn't declare media_capabilities; check
            // its schema for produced RuntimeDataType::Video / ::Audio.
            use remotemedia_core::nodes::schema::RuntimeDataType;
            let Some(schema) = factory.schema() else {
                continue;
            };
            for produced in &schema.produces {
                match produced {
                    RuntimeDataType::Video => {
                        let stream_id = node
                            .params
                            .get("video_stream_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string();
                        if !seen_video.insert(stream_id.clone()) {
                            tracing::warn!(
                                stream_id = %stream_id,
                                node_id = %node.id,
                                "scanner: duplicate video stream_id, keeping first"
                            );
                            continue;
                        }
                        let width = node
                            .params
                            .get("width")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(1024);
                        let height = node
                            .params
                            .get("height")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(1024);
                        let framerate = node
                            .params
                            .get("framerate")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(30);
                        plan.video_outputs.push(VideoStreamSpec {
                            stream_id,
                            width,
                            height,
                            framerate,
                        });
                    }
                    RuntimeDataType::Audio => {
                        let stream_id = node
                            .params
                            .get("audio_stream_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string();
                        if !seen_audio.insert(stream_id.clone()) {
                            tracing::warn!(
                                stream_id = %stream_id,
                                node_id = %node.id,
                                "scanner: duplicate audio stream_id, keeping first"
                            );
                            continue;
                        }
                        let sample_rate = node
                            .params
                            .get("sample_rate")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32)
                            .unwrap_or(48_000);
                        let channels = node
                            .params
                            .get("channels")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u16)
                            .unwrap_or(1);
                        plan.audio_outputs.push(AudioStreamSpec {
                            stream_id,
                            sample_rate,
                            channels,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    plan
}

fn exact_u32(c: &Option<ConstraintValue<u32>>) -> Option<u32> {
    match c {
        Some(ConstraintValue::Exact(v)) => Some(*v),
        _ => None,
    }
}

fn exact_f32_as_u32(c: &Option<ConstraintValue<f32>>) -> Option<u32> {
    match c {
        Some(ConstraintValue::Exact(v)) => Some(*v as u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::capabilities::{
        AudioConstraints, ConstraintValue, MediaCapabilities, MediaConstraints, VideoConstraints,
    };
    use remotemedia_core::manifest::{Manifest, ManifestMetadata, NodeManifest};
    use remotemedia_core::nodes::{
        streaming_node::StreamingNodeFactory, StreamingNode, StreamingNodeRegistry,
    };
    use remotemedia_core::Error;
    use serde_json::Value;
    use std::sync::Arc;

    /// Test factory: declares one video output with stream_id from params.
    struct VideoFactory;
    impl StreamingNodeFactory for VideoFactory {
        fn create(
            &self,
            _node_id: String,
            _params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>, Error> {
            unreachable!("scanner must not instantiate nodes")
        }
        fn node_type(&self) -> &str {
            "TestVideo"
        }
        fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
            Some(MediaCapabilities::with_output(MediaConstraints::Video(
                VideoConstraints::default(),
            )))
        }
    }

    /// Test factory: declares one audio output with explicit sample rate.
    struct AudioFactory;
    impl StreamingNodeFactory for AudioFactory {
        fn create(
            &self,
            _node_id: String,
            _params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>, Error> {
            unreachable!()
        }
        fn node_type(&self) -> &str {
            "TestAudio"
        }
        fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
            Some(MediaCapabilities::with_output(MediaConstraints::Audio(
                AudioConstraints {
                    sample_rate: Some(ConstraintValue::Exact(24_000u32)),
                    // AudioConstraints.channels is ConstraintValue<u32>, not u16.
                    channels: Some(ConstraintValue::Exact(1u32)),
                    format: None,
                },
            )))
        }
    }

    fn registry_with(factories: Vec<Arc<dyn StreamingNodeFactory>>) -> StreamingNodeRegistry {
        let mut r = StreamingNodeRegistry::new();
        for f in factories {
            r.register(f);
        }
        r
    }

    fn empty_manifest() -> Manifest {
        Manifest {
            version: "1.0".to_string(),
            metadata: ManifestMetadata::default(),
            nodes: Vec::new(),
            connections: Vec::new(),
            python_env: None,
            plugins: Vec::new(),
        }
    }

    fn manifest_with(nodes: Vec<NodeManifest>) -> Manifest {
        Manifest {
            version: "1.0".to_string(),
            metadata: ManifestMetadata::default(),
            nodes,
            connections: Vec::new(),
            python_env: None,
            plugins: Vec::new(),
        }
    }

    #[test]
    fn empty_manifest_yields_empty_plan() {
        let plan = scan(&empty_manifest(), &StreamingNodeRegistry::new());
        assert!(plan.audio_outputs.is_empty());
        assert!(plan.video_outputs.is_empty());
    }

    #[test]
    fn video_node_with_stream_id_param_emits_video_spec() {
        let registry = registry_with(vec![Arc::new(VideoFactory)]);
        let manifest = manifest_with(vec![NodeManifest {
            id: "renderer".into(),
            node_type: "TestVideo".into(),
            params: serde_json::json!({
                "video_stream_id": "avatar",
                "width": 512,
                "height": 512,
                "framerate": 30,
            }),
            ..Default::default()
        }]);

        let plan = scan(&manifest, &registry);

        assert_eq!(plan.video_outputs.len(), 1);
        let spec = &plan.video_outputs[0];
        assert_eq!(spec.stream_id, "avatar");
        assert_eq!(spec.width, 512);
        assert_eq!(spec.height, 512);
        assert_eq!(spec.framerate, 30);
        assert!(plan.audio_outputs.is_empty());
    }

    #[test]
    fn audio_node_without_stream_id_uses_default() {
        let registry = registry_with(vec![Arc::new(AudioFactory)]);
        let manifest = manifest_with(vec![NodeManifest {
            id: "tts".into(),
            node_type: "TestAudio".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }]);

        let plan = scan(&manifest, &registry);

        assert_eq!(plan.audio_outputs.len(), 1);
        let spec = &plan.audio_outputs[0];
        assert_eq!(spec.stream_id, "default");
        // sample_rate hint should come from the Exact constraint.
        assert_eq!(spec.sample_rate, 24_000);
        assert_eq!(spec.channels, 1);
    }

    #[test]
    fn unknown_node_type_is_skipped() {
        let manifest = manifest_with(vec![NodeManifest {
            id: "ghost".into(),
            node_type: "NoSuchNode".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }]);
        let plan = scan(&manifest, &StreamingNodeRegistry::new());
        assert!(plan.audio_outputs.is_empty());
        assert!(plan.video_outputs.is_empty());
    }

    /// Test factory: no media_capabilities, but schema declares video produces.
    struct SchemaOnlyVideoFactory;
    impl StreamingNodeFactory for SchemaOnlyVideoFactory {
        fn create(
            &self,
            _node_id: String,
            _params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>, Error> {
            unreachable!()
        }
        fn node_type(&self) -> &str {
            "SchemaOnlyVideo"
        }
        fn schema(&self) -> Option<remotemedia_core::nodes::schema::NodeSchema> {
            use remotemedia_core::nodes::schema::{NodeSchema, RuntimeDataType};
            Some(
                NodeSchema::new("SchemaOnlyVideo")
                    .accepts([RuntimeDataType::Json])
                    .produces([RuntimeDataType::Video]),
            )
        }
    }

    #[test]
    fn schema_fallback_emits_video_spec_when_no_caps() {
        let registry = registry_with(vec![Arc::new(SchemaOnlyVideoFactory)]);
        let manifest = manifest_with(vec![NodeManifest {
            id: "renderer".into(),
            node_type: "SchemaOnlyVideo".into(),
            params: serde_json::json!({ "video_stream_id": "avatar" }),
            ..Default::default()
        }]);

        let plan = scan(&manifest, &registry);

        assert_eq!(plan.video_outputs.len(), 1);
        assert_eq!(plan.video_outputs[0].stream_id, "avatar");
    }
}
