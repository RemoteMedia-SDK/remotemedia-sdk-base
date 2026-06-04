//! Conversion node registry (spec 022)
//!
//! This module provides a registry of conversion nodes that can be
//! automatically inserted to resolve capability mismatches.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::constraints::{
    AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints, VideoConstraints,
};
use super::negotiation::{ConversionPath, ConversionStep};

/// Render an `AudioSampleFormat` as the lowercase string the
/// `FastFormatConverterNodeFactory` parameter parser accepts.
fn format_as_param(fmt: AudioSampleFormat) -> &'static str {
    match fmt {
        AudioSampleFormat::F32 => "f32",
        AudioSampleFormat::I16 => "i16",
        AudioSampleFormat::I32 => "i32",
        AudioSampleFormat::U8 => "u8",
    }
}

// =============================================================================
// Converter Information
// =============================================================================

/// Information about a conversion node.
///
/// Used to describe what conversions a node can perform and how to configure it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConverterInfo {
    /// Node type that performs the conversion
    pub node_type: String,
    /// Media type this converter handles (e.g., "audio", "video")
    pub media_type: String,
    /// Which constraints this converter can change (e.g., ["sample_rate", "channels"])
    pub converts: Vec<String>,
    /// Default parameters for the converter
    pub default_params: serde_json::Value,
}

// =============================================================================
// Conversion Registry Trait
// =============================================================================

/// Registry of available conversion nodes.
///
/// Implementations provide methods to find converters for specific
/// constraint mismatches and to compute conversion paths.
pub trait ConversionRegistry: Send + Sync {
    /// Find converters that can handle a specific constraint mismatch.
    ///
    /// # Arguments
    ///
    /// * `media_type` - The media type (e.g., "audio", "video")
    /// * `constraint_name` - The constraint that needs conversion (e.g., "sample_rate")
    ///
    /// # Returns
    ///
    /// List of converters that can handle this conversion.
    fn find_converters(&self, media_type: &str, constraint_name: &str) -> Vec<&ConverterInfo>;

    /// Find the shortest conversion path between two capability sets.
    ///
    /// Implements FR-015: selects the path with the fewest nodes.
    ///
    /// # Arguments
    ///
    /// * `source_caps` - The source node's output constraints
    /// * `target_caps` - The target node's input requirements
    ///
    /// # Returns
    ///
    /// `Some(path)` if a conversion path exists, `None` otherwise.
    fn find_conversion_path(
        &self,
        source_caps: &MediaConstraints,
        target_caps: &MediaConstraints,
    ) -> Option<ConversionPath>;

    /// Register a new converter.
    ///
    /// # Arguments
    ///
    /// * `info` - Information about the converter to register
    fn register_converter(&mut self, info: ConverterInfo);
}

// =============================================================================
// Default Conversion Registry
// =============================================================================

/// Default implementation of the conversion registry.
///
/// Pre-populated with common audio and video converters.
pub struct DefaultConversionRegistry {
    /// Converters indexed by (media_type, constraint_name)
    converters: HashMap<(String, String), Vec<ConverterInfo>>,
}

impl DefaultConversionRegistry {
    /// Create a new registry with pre-registered converters.
    pub fn new() -> Self {
        let mut registry = Self {
            converters: HashMap::new(),
        };

        // FastResampleNode (rubato sinc resampler) handles sample-rate and
        // channel-count conversion in a single pass.
        registry.register_converter(ConverterInfo {
            node_type: "FastResampleNode".to_string(),
            media_type: "audio".to_string(),
            converts: vec!["sample_rate".to_string(), "channels".to_string()],
            default_params: serde_json::json!({}),
        });

        // FastFormatConverterNode wraps the F32/I16/I32 converter for the
        // streaming pipeline. See its docs for the data-plane caveat — the
        // streaming transport is F32-only today, so this node runs as a
        // quantize-then-dequantize roundtrip to honor declared format caps.
        registry.register_converter(ConverterInfo {
            node_type: "FastFormatConverterNode".to_string(),
            media_type: "audio".to_string(),
            converts: vec!["format".to_string()],
            default_params: serde_json::json!({}),
        });

        // Register video converters
        registry.register_converter(ConverterInfo {
            node_type: "VideoScale".to_string(),
            media_type: "video".to_string(),
            converts: vec!["width".to_string(), "height".to_string()],
            default_params: serde_json::json!({}),
        });

        registry.register_converter(ConverterInfo {
            node_type: "VideoFramerate".to_string(),
            media_type: "video".to_string(),
            converts: vec!["framerate".to_string()],
            default_params: serde_json::json!({}),
        });

        registry.register_converter(ConverterInfo {
            node_type: "VideoFormatConvert".to_string(),
            media_type: "video".to_string(),
            converts: vec!["pixel_format".to_string()],
            default_params: serde_json::json!({}),
        });

        registry
    }

    /// Create an empty registry (for testing).
    pub fn empty() -> Self {
        Self {
            converters: HashMap::new(),
        }
    }

    /// Find mismatched constraints between source and target audio constraints.
    fn find_audio_mismatches(source: &AudioConstraints, target: &AudioConstraints) -> Vec<String> {
        let mut mismatches = Vec::new();

        // Check sample_rate
        if let (Some(src), Some(tgt)) = (&source.sample_rate, &target.sample_rate) {
            if !src.compatible_with(tgt) {
                mismatches.push("sample_rate".to_string());
            }
        }

        // Check channels
        if let (Some(src), Some(tgt)) = (&source.channels, &target.channels) {
            if !src.compatible_with(tgt) {
                mismatches.push("channels".to_string());
            }
        }

        // Check format
        if let (Some(src), Some(tgt)) = (&source.format, &target.format) {
            if !src.compatible_with(tgt) {
                mismatches.push("format".to_string());
            }
        }

        mismatches
    }

    /// Find mismatched constraints between source and target video constraints.
    fn find_video_mismatches(source: &VideoConstraints, target: &VideoConstraints) -> Vec<String> {
        let mut mismatches = Vec::new();

        // Check width
        if let (Some(src), Some(tgt)) = (&source.width, &target.width) {
            if !src.compatible_with(tgt) {
                mismatches.push("width".to_string());
            }
        }

        // Check height
        if let (Some(src), Some(tgt)) = (&source.height, &target.height) {
            if !src.compatible_with(tgt) {
                mismatches.push("height".to_string());
            }
        }

        // Check framerate (use partial comparison since f32 doesn't implement Ord)
        if let (Some(src), Some(tgt)) = (&source.framerate, &target.framerate) {
            if !src.compatible_with_partial(tgt) {
                mismatches.push("framerate".to_string());
            }
        }

        // Check pixel_format
        if let (Some(src), Some(tgt)) = (&source.pixel_format, &target.pixel_format) {
            if !src.compatible_with(tgt) {
                mismatches.push("pixel_format".to_string());
            }
        }

        mismatches
    }

    /// Build a single FastResampleNode step bridging `source` → `target`.
    ///
    /// Reads exact values from the source/target constraints when present
    /// (`source_rate`, `target_rate`, `channels`) and falls back to `"auto"`
    /// strings — which FastResampleNodeFactory accepts as a signal to defer
    /// the value to its `AutoResampleStreamingNode` path. The resulting node
    /// covers rate and channel mismatches in one hop; sample-format mismatches
    /// are unsupported and rejected earlier in `find_conversion_path`.
    fn create_resample_step(
        source: &AudioConstraints,
        target: &AudioConstraints,
    ) -> ConversionStep {
        let mut params = serde_json::Map::new();
        match &source.sample_rate {
            Some(ConstraintValue::Exact(rate)) => {
                params.insert("source_rate".into(), serde_json::json!(rate));
            }
            _ => {
                params.insert("source_rate".into(), serde_json::json!("auto"));
            }
        }
        match &target.sample_rate {
            Some(ConstraintValue::Exact(rate)) => {
                params.insert("target_rate".into(), serde_json::json!(rate));
            }
            _ => {
                params.insert("target_rate".into(), serde_json::json!("auto"));
            }
        }
        if let Some(ConstraintValue::Exact(ch)) = &target.channels {
            params.insert("channels".into(), serde_json::json!(ch));
        }
        params.insert("quality".into(), serde_json::json!("Medium"));

        // Input caps mirror the source side (whatever the upstream emits),
        // output caps lock to the target so downstream validation passes.
        // Format stays F32 — that's all FastResampleNode produces.
        let output = AudioConstraints {
            sample_rate: target.sample_rate.clone(),
            channels: target.channels.clone(),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        };

        ConversionStep {
            node_type: "FastResampleNode".to_string(),
            params: serde_json::Value::Object(params),
            input_caps: MediaConstraints::Audio(source.clone()),
            output_caps: MediaConstraints::Audio(output),
        }
    }

    /// Build a single FastFormatConverterNode step bridging the source format
    /// to the target's exact format. Sample rate and channel constraints flow
    /// through unchanged — this step only changes the declared format.
    ///
    /// `target.format` must be `Some(Exact(_))` for this to produce a useful
    /// step; callers should only invoke it after `find_audio_mismatches`
    /// reports a `"format"` mismatch.
    fn create_format_step(
        source_before: &AudioConstraints,
        target: &AudioConstraints,
    ) -> Option<ConversionStep> {
        let target_fmt = match &target.format {
            Some(ConstraintValue::Exact(fmt)) => *fmt,
            _ => return None, // Nothing to lock to; skip.
        };

        let mut params = serde_json::Map::new();
        params.insert(
            "target_format".into(),
            serde_json::json!(format_as_param(target_fmt)),
        );
        if let Some(ConstraintValue::Exact(src_fmt)) = &source_before.format {
            params.insert(
                "source_format".into(),
                serde_json::json!(format_as_param(*src_fmt)),
            );
        }

        let output = AudioConstraints {
            sample_rate: source_before.sample_rate.clone(),
            channels: source_before.channels.clone(),
            format: Some(ConstraintValue::Exact(target_fmt)),
        };

        Some(ConversionStep {
            node_type: "FastFormatConverterNode".to_string(),
            params: serde_json::Value::Object(params),
            input_caps: MediaConstraints::Audio(source_before.clone()),
            output_caps: MediaConstraints::Audio(output),
        })
    }

    /// Create a conversion step for a video constraint.
    fn create_video_conversion_step(
        constraint: &str,
        target: &VideoConstraints,
    ) -> Option<ConversionStep> {
        let node_type = match constraint {
            "width" | "height" => "VideoScale",
            "framerate" => "VideoFramerate",
            "pixel_format" => "VideoFormatConvert",
            _ => return None,
        };

        // Build params based on target constraint
        let params = match constraint {
            "width" | "height" => {
                let mut p = serde_json::Map::new();
                if let Some(ConstraintValue::Exact(w)) = &target.width {
                    p.insert("target_width".to_string(), serde_json::json!(w));
                }
                if let Some(ConstraintValue::Exact(h)) = &target.height {
                    p.insert("target_height".to_string(), serde_json::json!(h));
                }
                serde_json::Value::Object(p)
            }
            "framerate" => {
                if let Some(ConstraintValue::Exact(fps)) = &target.framerate {
                    serde_json::json!({"target_framerate": fps})
                } else {
                    serde_json::json!({})
                }
            }
            "pixel_format" => {
                if let Some(ConstraintValue::Exact(fmt)) = &target.pixel_format {
                    serde_json::json!({"target_format": format!("{:?}", fmt)})
                } else {
                    serde_json::json!({})
                }
            }
            _ => serde_json::json!({}),
        };

        Some(ConversionStep {
            node_type: node_type.to_string(),
            params,
            input_caps: MediaConstraints::Video(VideoConstraints::default()),
            output_caps: MediaConstraints::Video(target.clone()),
        })
    }
}

impl Default for DefaultConversionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConversionRegistry for DefaultConversionRegistry {
    fn find_converters(&self, media_type: &str, constraint_name: &str) -> Vec<&ConverterInfo> {
        self.converters
            .get(&(media_type.to_string(), constraint_name.to_string()))
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    fn find_conversion_path(
        &self,
        source_caps: &MediaConstraints,
        target_caps: &MediaConstraints,
    ) -> Option<ConversionPath> {
        // Must be same media type
        if source_caps.media_type() != target_caps.media_type() {
            return None;
        }

        let mut steps = Vec::new();

        match (source_caps, target_caps) {
            (MediaConstraints::Audio(source), MediaConstraints::Audio(target)) => {
                let mismatches = Self::find_audio_mismatches(source, target);

                if mismatches.is_empty() {
                    return Some(ConversionPath::empty());
                }

                let needs_resample = mismatches
                    .iter()
                    .any(|m| m == "sample_rate" || m == "channels");
                let needs_format = mismatches.iter().any(|m| m == "format");

                // Chain order: resample first (so the converter quantizes the
                // resampled values, not the raw ones), format second. When
                // both are required we emit two steps with the intermediate
                // constraint reflecting "post-resample, pre-format".
                let mut current_source = source.clone();
                if needs_resample {
                    let step = Self::create_resample_step(&current_source, target);
                    // The resample step's output is what the format step's
                    // input must mirror; clone it forward.
                    if let MediaConstraints::Audio(post) = &step.output_caps {
                        current_source = post.clone();
                    }
                    steps.push(step);
                }
                if needs_format {
                    match Self::create_format_step(&current_source, target) {
                        Some(step) => steps.push(step),
                        // Target's format constraint isn't `Exact` — we can't
                        // pick a concrete target_format, so the path is
                        // unbridgeable. Drop any earlier resample step we may
                        // have queued; the caller surfaces the mismatch.
                        None => return None,
                    }
                }
            }
            (MediaConstraints::Video(source), MediaConstraints::Video(target)) => {
                let mismatches = Self::find_video_mismatches(source, target);

                if mismatches.is_empty() {
                    return Some(ConversionPath::empty());
                }

                // Handle width+height together
                let has_scale = mismatches.iter().any(|m| m == "width" || m == "height");
                if has_scale {
                    if let Some(step) = Self::create_video_conversion_step("width", target) {
                        steps.push(step);
                    }
                }

                // Handle other constraints
                for mismatch in &mismatches {
                    if mismatch != "width" && mismatch != "height" {
                        if let Some(step) = Self::create_video_conversion_step(mismatch, target) {
                            steps.push(step);
                        }
                    }
                }
            }
            _ => {
                // Other media types don't have conversion support yet
                return None;
            }
        }

        if steps.is_empty() {
            None
        } else {
            Some(ConversionPath {
                total_nodes: steps.len(),
                estimated_latency_us: None,
                steps,
            })
        }
    }

    fn register_converter(&mut self, info: ConverterInfo) {
        for constraint in &info.converts {
            self.converters
                .entry((info.media_type.clone(), constraint.clone()))
                .or_insert_with(Vec::new)
                .push(info.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::constraints::AudioSampleFormat;
    use super::*;

    #[test]
    fn test_default_registry_has_converters() {
        let registry = DefaultConversionRegistry::new();

        let audio_resamplers = registry.find_converters("audio", "sample_rate");
        assert!(!audio_resamplers.is_empty());
        assert!(audio_resamplers
            .iter()
            .any(|c| c.node_type == "FastResampleNode"));

        // Channels mismatch is also handled by FastResampleNode.
        let audio_channels = registry.find_converters("audio", "channels");
        assert!(audio_channels
            .iter()
            .any(|c| c.node_type == "FastResampleNode"));

        let video_scalers = registry.find_converters("video", "width");
        assert!(!video_scalers.is_empty());
        assert!(video_scalers.iter().any(|c| c.node_type == "VideoScale"));
    }

    #[test]
    fn test_find_conversion_path_audio_sample_rate() {
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: None,
            format: None,
        });

        let target = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16000)),
            channels: None,
            format: None,
        });

        let path = registry.find_conversion_path(&source, &target);
        assert!(path.is_some());

        let path = path.unwrap();
        assert_eq!(path.total_nodes, 1);
        assert_eq!(path.steps[0].node_type, "FastResampleNode");
        assert_eq!(path.steps[0].params["source_rate"], 48000);
        assert_eq!(path.steps[0].params["target_rate"], 16000);
    }

    #[test]
    fn test_find_conversion_path_rate_and_channels_collapse_to_one_step() {
        // FastResampleNode handles rate + channels in one pass, so even when
        // both are mismatched the path should be a single step.
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        });

        let target = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        });

        let path = registry.find_conversion_path(&source, &target).unwrap();
        assert_eq!(path.total_nodes, 1);
        assert_eq!(path.steps[0].node_type, "FastResampleNode");
        assert_eq!(path.steps[0].params["target_rate"], 16000);
        assert_eq!(path.steps[0].params["channels"], 1);
    }

    #[test]
    fn test_find_conversion_path_format_only_emits_format_converter() {
        // Format mismatch with matching rate/channels is bridged by a single
        // FastFormatConverterNode step.
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::I16)),
        });
        let target = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        });

        let path = registry.find_conversion_path(&source, &target).unwrap();
        assert_eq!(path.total_nodes, 1);
        assert_eq!(path.steps[0].node_type, "FastFormatConverterNode");
        assert_eq!(path.steps[0].params["target_format"], "f32");
        assert_eq!(path.steps[0].params["source_format"], "i16");
    }

    #[test]
    fn test_find_conversion_path_rate_and_format_chain() {
        // Rate + format mismatch produces a two-step chain: resample then
        // format conversion. Order matters — quantization should happen after
        // resampling so we don't waste precision on samples we discard.
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::I16)),
        });
        let target = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        });

        let path = registry.find_conversion_path(&source, &target).unwrap();
        assert_eq!(path.total_nodes, 2);
        assert_eq!(path.steps[0].node_type, "FastResampleNode");
        assert_eq!(path.steps[1].node_type, "FastFormatConverterNode");
        assert_eq!(path.steps[0].params["target_rate"], 16_000);
        assert_eq!(path.steps[1].params["target_format"], "f32");
    }

    #[test]
    fn test_find_conversion_path_compatible() {
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: None,
            format: None,
        });

        let target = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: None,
            format: None,
        });

        let path = registry.find_conversion_path(&source, &target);
        assert!(path.is_some());
        assert!(path.unwrap().is_empty());
    }

    #[test]
    fn test_find_conversion_path_different_media_types() {
        let registry = DefaultConversionRegistry::new();

        let source = MediaConstraints::Audio(AudioConstraints::default());
        let target = MediaConstraints::Video(VideoConstraints::default());

        let path = registry.find_conversion_path(&source, &target);
        assert!(path.is_none());
    }

    #[test]
    fn test_register_custom_converter() {
        let mut registry = DefaultConversionRegistry::empty();

        registry.register_converter(ConverterInfo {
            node_type: "CustomResampler".to_string(),
            media_type: "audio".to_string(),
            converts: vec!["sample_rate".to_string()],
            default_params: serde_json::json!({}),
        });

        let converters = registry.find_converters("audio", "sample_rate");
        assert_eq!(converters.len(), 1);
        assert_eq!(converters[0].node_type, "CustomResampler");
    }
}
