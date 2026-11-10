use super::*;
use remotemedia_types::{ControlMessageType, CopyCounts, CopyPolicy};

fn strict() -> CopyContext {
    CopyContext::new(CopyPolicy::RequireZeroCopy, "ffi-control-test")
}

fn metadata() -> Value {
    serde_json::json!({
        "counter": u64::MAX,
        "nested": [{"__pickled__": true, "data": "literal, not a pickle"},
                   {"__numpy__": true, "array": "literal, not an array"}],
        "participant.id": "kept-in-control-metadata",
    })
}

#[test]
fn every_control_variant_roundtrips_exactly_without_bulk_materialization() {
    Python::attach(|py| {
        for message_type in [
            ControlMessageType::CancelSpeculation {
                from_timestamp: u64::MAX - 1,
                to_timestamp: u64::MAX,
            },
            ControlMessageType::BatchHint {
                suggested_batch_size: usize::MAX,
            },
            ControlMessageType::DeadlineWarning {
                deadline_us: u64::MAX,
            },
        ] {
            let data = RuntimeData::ControlMessage {
                message_type,
                segment_id: Some("segment-1".into()),
                timestamp_ms: u64::MAX,
                metadata: metadata(),
            };
            let context = strict();
            let output = runtime_data_to_python_with_context(py, &data, &context).unwrap();
            assert_eq!(
                output
                    .bind(py)
                    .get_item("timestamp_ms")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                u64::MAX
            );
            let recovered =
                python_to_runtime_data_with_context(py, output.bind(py), &context).unwrap();
            assert_eq!(
                serde_json::to_value(recovered).unwrap(),
                serde_json::to_value(data).unwrap()
            );
            assert!(context
                .report()
                .entries
                .iter()
                .all(|(_, count)| *count == CopyCounts::default()));
        }
    });
}

#[test]
fn malformed_controls_fail_instead_of_becoming_ordinary_json() {
    Python::attach(|py| {
        let valid = serde_json::json!({"type":"control_message", "message_type":{"type":"BatchHint", "suggested_batch_size":4}, "timestamp_ms":1, "metadata":null});
        for (field, value) in [
            ("unexpected", serde_json::json!(true)),
            ("timestamp_ms", serde_json::json!(-1)),
            ("timestamp_ms", serde_json::json!(1.5)),
            ("message_type", serde_json::json!({"type":"Unknown"})),
            (
                "message_type",
                serde_json::json!({"type":"CancelSpeculation", "from_timestamp":1}),
            ),
            (
                "message_type",
                serde_json::json!({"type":"BatchHint", "suggested_batch_size":-1}),
            ),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            let input = buffers::metadata_to_python(py, &invalid).unwrap();
            for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
                let context = CopyContext::new(policy, "ffi-invalid-control-test");
                assert!(python_to_runtime_data_with_context(py, &input, &context).is_err());
                assert!(context
                    .report()
                    .entries
                    .iter()
                    .all(|(_, count)| *count == CopyCounts::default()));
            }
        }
        let input = buffers::metadata_to_python(py, &valid).unwrap();
        assert!(matches!(
            python_to_runtime_data_with_context(py, &input, &strict()).unwrap(),
            RuntimeData::ControlMessage {
                segment_id: None,
                ..
            }
        ));
    });
}

#[test]
fn control_metadata_rejects_hidden_python_payloads_without_packing() {
    Python::attach(|py| {
        let input = PyDict::new(py);
        input.set_item("type", "control_message").unwrap();
        input
            .set_item(
                "metadata",
                super::buffer_tests::array(py, "np.zeros(8, dtype=np.float32)"),
            )
            .unwrap();
        let context = strict();
        let error = python_to_runtime_data_with_context(py, input.as_any(), &context).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        assert!(context
            .report()
            .entries
            .iter()
            .all(|(_, count)| *count == CopyCounts::default()));
    });
}

#[test]
fn media_metadata_remains_literal_while_its_payload_retains_identity() {
    Python::attach(|py| {
        let context = strict();
        let data = RuntimeData::Audio {
            samples: remotemedia_types::SharedBuffer::from(vec![1.0f32, 2.0]).into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: Some(u64::MAX),
            arrival_ts_us: None,
            metadata: Some(metadata()),
        };
        let RuntimeData::Audio { samples, .. } = &data else {
            unreachable!()
        };
        let pointer = samples.as_ptr();
        let output = runtime_data_to_python_with_context(py, &data, &context).unwrap();
        let recovered = python_to_runtime_data_with_context(py, output.bind(py), &context).unwrap();
        let RuntimeData::Audio {
            samples,
            timestamp_us,
            metadata: result,
            ..
        } = recovered
        else {
            unreachable!()
        };
        assert_eq!(samples.as_ptr(), pointer);
        assert_eq!(timestamp_us, Some(u64::MAX));
        assert_eq!(result, Some(metadata()));
        assert!(context
            .report()
            .entries
            .iter()
            .all(|(_, count)| *count == CopyCounts::default()));
    });
}
