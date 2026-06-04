//! Re-exports of media constraint types from `remotemedia-traits`.
//!
//! These types moved out of core in Task A4 so plugin authors can
//! declare capabilities (`MediaCapabilities`, `MediaConstraints`, audio /
//! video / tensor / text / file / json constraints, `ConstraintValue<T>`,
//! `AudioSampleFormat`, `PixelFormat`, `TensorDataType`) without
//! depending on the heavy host crate.
//!
//! Resolver / negotiation / validation logic stays in core under
//! sibling modules.

pub use remotemedia_traits::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityPixelFormat, ConstraintValue, FileConstraints,
    JsonConstraints, MediaCapabilities, MediaConstraints, PixelFormat, TensorConstraints,
    TensorDataType, TextConstraints, VideoConstraints,
};

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // ConstraintValue tests
    // =========================================================================

    #[test]
    fn test_constraint_value_satisfies_exact() {
        let constraint = ConstraintValue::Exact(48000u32);
        assert!(constraint.satisfies(&48000));
        assert!(!constraint.satisfies(&16000));
    }

    #[test]
    fn test_constraint_value_satisfies_range() {
        let constraint = ConstraintValue::Range {
            min: 16000u32,
            max: 48000,
        };
        assert!(constraint.satisfies(&16000));
        assert!(constraint.satisfies(&32000));
        assert!(constraint.satisfies(&48000));
        assert!(!constraint.satisfies(&8000));
        assert!(!constraint.satisfies(&96000));
    }

    #[test]
    fn test_constraint_value_satisfies_set() {
        let constraint = ConstraintValue::Set(vec![16000u32, 44100, 48000]);
        assert!(constraint.satisfies(&16000));
        assert!(constraint.satisfies(&44100));
        assert!(constraint.satisfies(&48000));
        assert!(!constraint.satisfies(&22050));
    }

    #[test]
    fn test_constraint_value_is_flexible() {
        assert!(!ConstraintValue::Exact(48000u32).is_flexible());
        assert!(ConstraintValue::Range {
            min: 16000u32,
            max: 48000
        }
        .is_flexible());
        assert!(ConstraintValue::Set(vec![16000u32, 48000]).is_flexible());
    }

    #[test]
    fn test_constraint_value_compatible_exact_exact() {
        let a = ConstraintValue::Exact(48000u32);
        let b = ConstraintValue::Exact(48000u32);
        let c = ConstraintValue::Exact(16000u32);

        assert!(a.compatible_with(&b));
        assert!(!a.compatible_with(&c));
    }

    #[test]
    fn test_constraint_value_compatible_exact_range() {
        let exact = ConstraintValue::Exact(32000u32);
        let range = ConstraintValue::Range {
            min: 16000,
            max: 48000,
        };
        let out_of_range = ConstraintValue::Exact(8000u32);

        assert!(exact.compatible_with(&range));
        assert!(range.compatible_with(&exact));
        assert!(!out_of_range.compatible_with(&range));
    }

    #[test]
    fn test_constraint_value_compatible_range_range() {
        let r1 = ConstraintValue::Range {
            min: 16000u32,
            max: 48000,
        };
        let r2 = ConstraintValue::Range {
            min: 32000,
            max: 96000,
        };
        let r3 = ConstraintValue::Range {
            min: 64000,
            max: 96000,
        };

        assert!(r1.compatible_with(&r2)); // Overlap at 32000-48000
        assert!(!r1.compatible_with(&r3)); // No overlap
    }

    #[test]
    fn test_constraint_value_json_exact() {
        let constraint = ConstraintValue::Exact(48000u32);
        let json = serde_json::to_string(&constraint).unwrap();
        assert_eq!(json, "48000");

        let parsed: ConstraintValue<u32> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, constraint);
    }

    #[test]
    fn test_constraint_value_json_range() {
        let constraint = ConstraintValue::Range {
            min: 16000u32,
            max: 48000,
        };
        let json = serde_json::to_string(&constraint).unwrap();
        assert!(json.contains("\"min\""));
        assert!(json.contains("\"max\""));

        let parsed: ConstraintValue<u32> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, constraint);
    }

    #[test]
    fn test_constraint_value_json_set() {
        let constraint = ConstraintValue::Set(vec![16000u32, 44100, 48000]);
        let json = serde_json::to_string(&constraint).unwrap();
        assert_eq!(json, "[16000,44100,48000]");

        let parsed: ConstraintValue<u32> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, constraint);
    }

    #[test]
    fn test_audio_constraints_json() {
        let constraints = AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Range { min: 1, max: 2 }),
            format: Some(ConstraintValue::Set(vec![
                AudioSampleFormat::F32,
                AudioSampleFormat::I16,
            ])),
        };

        let json = serde_json::to_string(&constraints).unwrap();
        let parsed: AudioConstraints = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, constraints);
    }

    #[test]
    fn test_media_constraints_json_tagged() {
        let constraints = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: None,
        });

        let json = serde_json::to_string(&constraints).unwrap();
        assert!(json.contains("\"type\":\"audio\""));

        let parsed: MediaConstraints = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, constraints);
    }

    #[test]
    fn test_media_capabilities_accepts_any() {
        let empty = MediaCapabilities::default();
        assert!(empty.accepts_any());
        assert!(empty.output_unspecified());

        let with_input =
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints::default()));
        assert!(!with_input.accepts_any());
        assert!(with_input.output_unspecified());
    }

    #[test]
    fn test_media_capabilities_with_input_output() {
        let caps = MediaCapabilities::with_input_output(
            MediaConstraints::Audio(AudioConstraints::default()),
            MediaConstraints::Text(TextConstraints::default()),
        );

        assert!(!caps.accepts_any());
        assert!(!caps.output_unspecified());
        assert!(caps.default_input().is_some());
        assert!(caps.default_output().is_some());
    }

    #[test]
    fn test_media_constraints_media_type() {
        assert_eq!(
            MediaConstraints::Audio(AudioConstraints::default()).media_type(),
            "audio"
        );
        assert_eq!(
            MediaConstraints::Video(VideoConstraints::default()).media_type(),
            "video"
        );
        assert_eq!(MediaConstraints::Binary.media_type(), "binary");
    }
}
