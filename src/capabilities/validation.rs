//! Capability validation for pipeline connections (spec 022)
//!
//! This module provides validation logic for checking compatibility between
//! connected nodes' capabilities. It implements FR-009 through FR-012.

use std::fmt;

use super::constraints::{
    AudioConstraints, ConstraintValue, FileConstraints, JsonConstraints, MediaCapabilities,
    MediaConstraints, TensorConstraints, TextConstraints, VideoConstraints,
};

// =============================================================================
// Capability Mismatch (FR-010, FR-011)
// =============================================================================

/// Record of incompatibility between connected nodes.
///
/// Contains all information needed to generate a user-friendly error message
/// that identifies the specific nodes and constraint conflict (FR-011).
#[derive(Debug, Clone, PartialEq)]
pub struct CapabilityMismatch {
    /// ID of the source node
    pub source_node: String,
    /// ID of the target node
    pub target_node: String,
    /// Output port name on source
    pub source_port: String,
    /// Input port name on target
    pub target_port: String,
    /// Type of media involved in the mismatch
    pub media_type: String,
    /// Specific constraint that mismatched (e.g., "sample_rate")
    pub constraint_name: String,
    /// Human-readable representation of source's output value
    pub source_value: String,
    /// Human-readable representation of target's requirement
    pub target_requirement: String,
    /// Optional suggestion for resolving the mismatch (spec 023)
    pub suggestion: Option<String>,
}

impl CapabilityMismatch {
    /// Create a formatted error message for display (SC-003).
    ///
    /// The message format is designed to be understood on first reading
    /// without needing additional documentation lookup.
    pub fn display_message(&self) -> String {
        let base = format!(
            "Capability mismatch: {} → {}\n\
             \x20 Media type: {}\n\
             \x20 Constraint: {}\n\
             \x20 Source outputs: {}\n\
             \x20 Target requires: {}",
            self.source_node,
            self.target_node,
            self.media_type,
            self.constraint_name,
            self.source_value,
            self.target_requirement
        );

        if let Some(ref suggestion) = self.suggestion {
            format!("{}\n\x20 Suggestion: {}", base, suggestion)
        } else {
            base
        }
    }

    /// Generate a suggestion for resolving this mismatch.
    ///
    /// Returns a human-readable suggestion string based on the mismatch type.
    pub fn generate_suggestion(&self) -> Option<String> {
        match (self.media_type.as_str(), self.constraint_name.as_str()) {
            ("audio", "sample_rate") => Some(format!(
                "Insert AudioResample node between '{}' and '{}' to convert {} Hz to {} Hz",
                self.source_node, self.target_node, self.source_value, self.target_requirement
            )),
            ("audio", "channels") => Some(format!(
                "Insert ChannelMixer node between '{}' and '{}' to convert {} channel(s) to {} channel(s)",
                self.source_node, self.target_node, self.source_value, self.target_requirement
            )),
            ("audio", "format") => Some(format!(
                "Insert AudioFormatConvert node between '{}' and '{}' to convert {} to {}",
                self.source_node, self.target_node, self.source_value, self.target_requirement
            )),
            ("video", "width" | "height") => Some(format!(
                "Insert VideoResize node between '{}' and '{}' to resize video",
                self.source_node, self.target_node
            )),
            ("video", "framerate") => Some(format!(
                "Insert FrameRateConvert node between '{}' and '{}' to change framerate",
                self.source_node, self.target_node
            )),
            ("video", "pixel_format") => Some(format!(
                "Insert PixelFormatConvert node between '{}' and '{}' to convert pixel format",
                self.source_node, self.target_node
            )),
            ("type", "media_type") => Some(format!(
                "Cannot connect {} ({}) to {} ({}). These nodes have incompatible media types.",
                self.source_node, self.source_value, self.target_node, self.target_requirement
            )),
            _ => None,
        }
    }

    /// Create a mismatch with auto-generated suggestion.
    pub fn with_auto_suggestion(mut self) -> Self {
        self.suggestion = self.generate_suggestion();
        self
    }
}

impl fmt::Display for CapabilityMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_message())
    }
}

// =============================================================================
// Validation Result (FR-009, FR-010)
// =============================================================================

/// Result of capability validation.
///
/// Implements FR-010: reports all mismatches, not just the first one found.
#[derive(Debug, Clone)]
pub enum CapabilityValidationResult {
    /// All capabilities are compatible - pipeline is valid
    Valid,
    /// Capabilities are incompatible with these mismatches
    Invalid(Vec<CapabilityMismatch>),
}

impl CapabilityValidationResult {
    /// Check if validation passed.
    pub fn is_valid(&self) -> bool {
        matches!(self, CapabilityValidationResult::Valid)
    }

    /// Get all mismatches (if any).
    pub fn mismatches(&self) -> &[CapabilityMismatch] {
        match self {
            CapabilityValidationResult::Valid => &[],
            CapabilityValidationResult::Invalid(m) => m,
        }
    }

    /// Get the number of mismatches.
    pub fn mismatch_count(&self) -> usize {
        self.mismatches().len()
    }
}

// =============================================================================
// Constraint Compatibility Checking (FR-009)
// =============================================================================

/// Check if a source output constraint is compatible with a target input requirement.
///
/// This is the core compatibility check used during pipeline validation.
/// Returns `None` if compatible, or `Some(mismatch_description)` if not.
fn check_constraint_compatible<T>(
    _constraint_name: &str,
    source: &Option<ConstraintValue<T>>,
    target: &Option<ConstraintValue<T>>,
) -> Option<(String, String)>
where
    T: PartialOrd + PartialEq + Clone + Ord + fmt::Debug,
{
    match (source, target) {
        // Target accepts any → always compatible
        (_, None) => None,

        // Source unspecified but target has requirement → might be incompatible
        // For now, treat as compatible (FR-008a: unspecified = passthrough)
        (None, Some(_)) => None,

        // Both specified → check compatibility
        (Some(src), Some(tgt)) => {
            if src.compatible_with(tgt) {
                None
            } else {
                Some((format!("{:?}", src), format!("{:?}", tgt)))
            }
        }
    }
}

/// Check if a source output constraint is compatible with a target input requirement.
///
/// This version works with types that only implement PartialOrd (like f32).
/// Returns `None` if compatible, or `Some(mismatch_description)` if not.
fn check_constraint_compatible_partial<T>(
    _constraint_name: &str,
    source: &Option<ConstraintValue<T>>,
    target: &Option<ConstraintValue<T>>,
) -> Option<(String, String)>
where
    T: PartialOrd + PartialEq + Clone + fmt::Debug,
{
    match (source, target) {
        // Target accepts any → always compatible
        (_, None) => None,

        // Source unspecified but target has requirement → might be incompatible
        // For now, treat as compatible (FR-008a: unspecified = passthrough)
        (None, Some(_)) => None,

        // Both specified → check compatibility
        (Some(src), Some(tgt)) => {
            if src.compatible_with_partial(tgt) {
                None
            } else {
                Some((format!("{:?}", src), format!("{:?}", tgt)))
            }
        }
    }
}

/// Check if audio constraints are compatible.
fn check_audio_compatible(
    source: &AudioConstraints,
    target: &AudioConstraints,
) -> Vec<(String, String, String)> {
    let mut mismatches = Vec::new();

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("sample_rate", &source.sample_rate, &target.sample_rate)
    {
        mismatches.push(("sample_rate".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("channels", &source.channels, &target.channels)
    {
        mismatches.push(("channels".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("format", &source.format, &target.format)
    {
        mismatches.push(("format".to_string(), src_val, tgt_val));
    }

    mismatches
}

/// Check if video constraints are compatible.
fn check_video_compatible(
    source: &VideoConstraints,
    target: &VideoConstraints,
) -> Vec<(String, String, String)> {
    let mut mismatches = Vec::new();

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("width", &source.width, &target.width)
    {
        mismatches.push(("width".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("height", &source.height, &target.height)
    {
        mismatches.push(("height".to_string(), src_val, tgt_val));
    }

    // Use partial comparison for f32 framerate (f32 doesn't implement Ord)
    if let Some((src_val, tgt_val)) =
        check_constraint_compatible_partial("framerate", &source.framerate, &target.framerate)
    {
        mismatches.push(("framerate".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("pixel_format", &source.pixel_format, &target.pixel_format)
    {
        mismatches.push(("pixel_format".to_string(), src_val, tgt_val));
    }

    mismatches
}

/// Check if tensor constraints are compatible.
fn check_tensor_compatible(
    source: &TensorConstraints,
    target: &TensorConstraints,
) -> Vec<(String, String, String)> {
    let mut mismatches = Vec::new();

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("dtype", &source.dtype, &target.dtype)
    {
        mismatches.push(("dtype".to_string(), src_val, tgt_val));
    }

    // Shape compatibility is complex - for now, just check if both are specified
    // and they have compatible dimensions
    if let (Some(src_shape), Some(tgt_shape)) = (&source.shape, &target.shape) {
        if !src_shape.compatible_with(tgt_shape) {
            mismatches.push((
                "shape".to_string(),
                format!("{:?}", src_shape),
                format!("{:?}", tgt_shape),
            ));
        }
    }

    mismatches
}

/// Check if text constraints are compatible.
fn check_text_compatible(
    source: &TextConstraints,
    target: &TextConstraints,
) -> Vec<(String, String, String)> {
    let mut mismatches = Vec::new();

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("encoding", &source.encoding, &target.encoding)
    {
        mismatches.push(("encoding".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("format", &source.format, &target.format)
    {
        mismatches.push(("format".to_string(), src_val, tgt_val));
    }

    mismatches
}

/// Check if file constraints are compatible.
fn check_file_compatible(
    source: &FileConstraints,
    target: &FileConstraints,
) -> Vec<(String, String, String)> {
    let mut mismatches = Vec::new();

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("extensions", &source.extensions, &target.extensions)
    {
        mismatches.push(("extensions".to_string(), src_val, tgt_val));
    }

    if let Some((src_val, tgt_val)) =
        check_constraint_compatible("mime_types", &source.mime_types, &target.mime_types)
    {
        mismatches.push(("mime_types".to_string(), src_val, tgt_val));
    }

    mismatches
}

/// Check if JSON constraints are compatible.
fn check_json_compatible(
    _source: &JsonConstraints,
    _target: &JsonConstraints,
) -> Vec<(String, String, String)> {
    // JSON schema compatibility is complex - for now, just accept any JSON
    // Full JSON Schema compatibility checking would require a schema validator
    Vec::new()
}

// =============================================================================
// Connection Validation (FR-009)
// =============================================================================

/// Validate a single connection between two nodes.
///
/// Returns a list of mismatches found between the source's output capabilities
/// and the target's input requirements.
pub fn validate_connection(
    source_id: &str,
    source_caps: &MediaCapabilities,
    target_id: &str,
    target_caps: &MediaCapabilities,
    source_port: &str,
    target_port: &str,
) -> Vec<CapabilityMismatch> {
    let mut mismatches = Vec::new();

    // Get the constraints for the specified ports
    let source_constraint = source_caps.outputs.get(source_port);
    let target_constraint = target_caps.inputs.get(target_port);

    // If target accepts any input, connection is valid
    if target_caps.accepts_any() || target_constraint.is_none() {
        return mismatches;
    }

    // If source output is unspecified, treat as passthrough (FR-008a)
    if source_caps.output_unspecified() || source_constraint.is_none() {
        return mismatches;
    }

    let source = source_constraint.unwrap();
    let target = target_constraint.unwrap();

    // Check media type compatibility first
    if source.media_type() != target.media_type() {
        let mismatch = CapabilityMismatch {
            source_node: source_id.to_string(),
            target_node: target_id.to_string(),
            source_port: source_port.to_string(),
            target_port: target_port.to_string(),
            media_type: "type".to_string(),
            constraint_name: "media_type".to_string(),
            source_value: source.media_type().to_string(),
            target_requirement: target.media_type().to_string(),
            suggestion: None,
        }
        .with_auto_suggestion();
        mismatches.push(mismatch);
        return mismatches;
    }

    // Check specific constraints based on media type
    let constraint_mismatches: Vec<(String, String, String)> = match (source, target) {
        (MediaConstraints::Audio(src), MediaConstraints::Audio(tgt)) => {
            check_audio_compatible(src, tgt)
        }
        (MediaConstraints::Video(src), MediaConstraints::Video(tgt)) => {
            check_video_compatible(src, tgt)
        }
        (MediaConstraints::Tensor(src), MediaConstraints::Tensor(tgt)) => {
            check_tensor_compatible(src, tgt)
        }
        (MediaConstraints::Text(src), MediaConstraints::Text(tgt)) => {
            check_text_compatible(src, tgt)
        }
        (MediaConstraints::File(src), MediaConstraints::File(tgt)) => {
            check_file_compatible(src, tgt)
        }
        (MediaConstraints::Json(src), MediaConstraints::Json(tgt)) => {
            check_json_compatible(src, tgt)
        }
        (MediaConstraints::Binary, MediaConstraints::Binary) => Vec::new(),
        _ => Vec::new(), // Already handled by media type check above
    };

    // Convert constraint mismatches to CapabilityMismatch structs with auto-suggestions
    for (constraint_name, source_value, target_requirement) in constraint_mismatches {
        let mismatch = CapabilityMismatch {
            source_node: source_id.to_string(),
            target_node: target_id.to_string(),
            source_port: source_port.to_string(),
            target_port: target_port.to_string(),
            media_type: source.media_type().to_string(),
            constraint_name,
            source_value,
            target_requirement,
            suggestion: None,
        }
        .with_auto_suggestion();
        mismatches.push(mismatch);
    }

    mismatches
}

/// Validate all connections in a pipeline.
///
/// Implements FR-010: checks all connections and collects all mismatches,
/// not just the first one found.
///
/// # Arguments
///
/// * `node_capabilities` - Map of node ID to its MediaCapabilities
/// * `connections` - List of (source_id, target_id) connections
///
/// # Returns
///
/// `CapabilityValidationResult::Valid` if all connections are compatible,
/// or `CapabilityValidationResult::Invalid` with all mismatches found.
pub fn validate_pipeline(
    node_capabilities: &std::collections::HashMap<String, MediaCapabilities>,
    connections: &[(String, String)],
) -> CapabilityValidationResult {
    let mut all_mismatches = Vec::new();

    for (source_id, target_id) in connections {
        // Get capabilities, defaulting to empty (accepts any) if not specified
        let source_caps = node_capabilities
            .get(source_id)
            .cloned()
            .unwrap_or_default();
        let target_caps = node_capabilities
            .get(target_id)
            .cloned()
            .unwrap_or_default();

        // Validate with default ports
        let mismatches = validate_connection(
            source_id,
            &source_caps,
            target_id,
            &target_caps,
            "default",
            "default",
        );

        all_mismatches.extend(mismatches);
    }

    if all_mismatches.is_empty() {
        CapabilityValidationResult::Valid
    } else {
        CapabilityValidationResult::Invalid(all_mismatches)
    }
}

/// Format all mismatches as a single error message.
///
/// Useful for displaying validation errors to users.
pub fn format_validation_errors(mismatches: &[CapabilityMismatch]) -> String {
    if mismatches.is_empty() {
        return "No capability mismatches found.".to_string();
    }

    let mut output = format!(
        "Validation found {} capability mismatch{}:\n\n",
        mismatches.len(),
        if mismatches.len() == 1 { "" } else { "es" }
    );

    for (i, mismatch) in mismatches.iter().enumerate() {
        output.push_str(&format!("{}. {}\n\n", i + 1, mismatch.display_message()));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::super::constraints::AudioSampleFormat;
    use super::*;

    // =========================================================================
    // Connection validation tests
    // =========================================================================

    #[test]
    fn test_validate_connection_compatible_audio() {
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        let target_caps =
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        let mismatches = validate_connection(
            "source",
            &source_caps,
            "target",
            &target_caps,
            "default",
            "default",
        );

        assert!(mismatches.is_empty());
    }

    #[test]
    fn test_validate_connection_incompatible_sample_rate() {
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            }));

        let target_caps =
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(16000)),
                channels: None,
                format: None,
            }));

        let mismatches = validate_connection(
            "source",
            &source_caps,
            "target",
            &target_caps,
            "default",
            "default",
        );

        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].constraint_name, "sample_rate");
        assert_eq!(mismatches[0].source_node, "source");
        assert_eq!(mismatches[0].target_node, "target");
    }

    #[test]
    fn test_validate_connection_target_accepts_any() {
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            }));

        let target_caps = MediaCapabilities::default(); // Accepts any

        let mismatches = validate_connection(
            "source",
            &source_caps,
            "target",
            &target_caps,
            "default",
            "default",
        );

        assert!(mismatches.is_empty());
    }

    #[test]
    fn test_validate_connection_media_type_mismatch() {
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints::default()));
        let target_caps =
            MediaCapabilities::with_input(MediaConstraints::Video(VideoConstraints::default()));

        let mismatches = validate_connection(
            "source",
            &source_caps,
            "target",
            &target_caps,
            "default",
            "default",
        );

        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].constraint_name, "media_type");
    }

    #[test]
    fn test_validate_connection_flexible_constraint() {
        // Source outputs exact 32kHz
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(32000)),
                channels: None,
                format: None,
            }));

        // Target accepts range 16kHz-48kHz
        let target_caps =
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 16000,
                    max: 48000,
                }),
                channels: None,
                format: None,
            }));

        let mismatches = validate_connection(
            "source",
            &source_caps,
            "target",
            &target_caps,
            "default",
            "default",
        );

        assert!(mismatches.is_empty()); // 32kHz is within 16k-48k range
    }

    // =========================================================================
    // Pipeline validation tests
    // =========================================================================

    #[test]
    fn test_validate_pipeline_valid() {
        use std::collections::HashMap;

        let mut caps = HashMap::new();
        caps.insert(
            "a".to_string(),
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            })),
        );
        caps.insert(
            "b".to_string(),
            MediaCapabilities::with_input_output(
                MediaConstraints::Audio(AudioConstraints {
                    sample_rate: Some(ConstraintValue::Exact(48000)),
                    channels: None,
                    format: None,
                }),
                MediaConstraints::Text(TextConstraints::default()),
            ),
        );

        let connections = vec![("a".to_string(), "b".to_string())];

        let result = validate_pipeline(&caps, &connections);
        assert!(result.is_valid());
    }

    #[test]
    fn test_validate_pipeline_multiple_mismatches() {
        use std::collections::HashMap;

        let mut caps = HashMap::new();
        caps.insert(
            "a".to_string(),
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: None,
            })),
        );
        caps.insert(
            "b".to_string(),
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(16000)),
                channels: Some(ConstraintValue::Exact(1)),
                format: None,
            })),
        );

        let connections = vec![("a".to_string(), "b".to_string())];

        let result = validate_pipeline(&caps, &connections);
        assert!(!result.is_valid());
        assert_eq!(result.mismatch_count(), 2); // sample_rate and channels
    }

    #[test]
    fn test_format_validation_errors() {
        let mismatches = vec![CapabilityMismatch {
            source_node: "audio_input".to_string(),
            target_node: "whisper".to_string(),
            source_port: "default".to_string(),
            target_port: "default".to_string(),
            media_type: "audio".to_string(),
            constraint_name: "sample_rate".to_string(),
            source_value: "48000".to_string(),
            target_requirement: "16000".to_string(),
            suggestion: None,
        }];

        let output = format_validation_errors(&mismatches);
        assert!(output.contains("1 capability mismatch"));
        assert!(output.contains("audio_input"));
        assert!(output.contains("whisper"));
        assert!(output.contains("sample_rate"));
    }

    #[test]
    fn test_mismatch_suggestion_generation() {
        let mismatch = CapabilityMismatch {
            source_node: "mic".to_string(),
            target_node: "whisper".to_string(),
            source_port: "default".to_string(),
            target_port: "default".to_string(),
            media_type: "audio".to_string(),
            constraint_name: "sample_rate".to_string(),
            source_value: "48000".to_string(),
            target_requirement: "16000".to_string(),
            suggestion: None,
        }
        .with_auto_suggestion();

        assert!(mismatch.suggestion.is_some());
        let suggestion = mismatch.suggestion.unwrap();
        assert!(suggestion.contains("AudioResample"));
        assert!(suggestion.contains("mic"));
        assert!(suggestion.contains("whisper"));
        assert!(suggestion.contains("48000"));
        assert!(suggestion.contains("16000"));
    }

    #[test]
    fn test_mismatch_display_with_suggestion() {
        let mismatch = CapabilityMismatch {
            source_node: "mic".to_string(),
            target_node: "whisper".to_string(),
            source_port: "default".to_string(),
            target_port: "default".to_string(),
            media_type: "audio".to_string(),
            constraint_name: "sample_rate".to_string(),
            source_value: "48000".to_string(),
            target_requirement: "16000".to_string(),
            suggestion: None,
        }
        .with_auto_suggestion();

        let display = mismatch.display_message();
        assert!(display.contains("Suggestion:"));
        assert!(display.contains("AudioResample"));
    }
}
