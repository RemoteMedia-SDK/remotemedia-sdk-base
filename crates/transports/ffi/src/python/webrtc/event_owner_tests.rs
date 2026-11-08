use super::*;
use pyo3::{buffer::PyUntypedBuffer, types::PySlice};
use remotemedia_types::{BufferBudget, CopyPolicy};

#[test]
fn observer_python_legacy_audio_promotes_without_copy() {
    Python::attach(|py| {
        let samples = vec![0.25f32, -0.5];
        let pointer = samples.as_ptr();
        let event = PipelineOutputEvent::from(crate::webrtc::events::PipelineOutputEvent {
            peer_id: "peer".into(),
            timestamp: 42,
            copies: CopyContext::legacy().clone(),
            data: RuntimeData::Audio {
                samples: remotemedia_types::AudioSamples::Vec(samples),
                sample_rate: 48000,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
        });
        let clone = event.clone();
        drop(event);
        assert!(clone.serialized.get().is_none());
        let value = clone.runtime_data(py).unwrap();
        assert!(clone.serialized.get().is_none());
        let samples = value.bind(py).get_item("samples").unwrap();
        let buffer = PyUntypedBuffer::get(&samples).unwrap();
        assert_eq!(buffer.buf_ptr() as *const f32, pointer);
        assert!(buffer.readonly());
    });
}

#[test]
fn observer_python_raw_views_retain_budget_and_bytes_are_explicit_copies() {
    Python::attach(|py| {
        let budget = BufferBudget::new(1, 16);
        let RuntimeData::Binary(source) = budget
            .admit(RuntimeData::Binary(vec![3; 16].into()))
            .unwrap()
        else {
            unreachable!()
        };
        let pointer = source.as_ptr();
        let event = DataReceivedEvent::from(crate::webrtc::events::DataReceivedEvent::new(
            "peer".into(),
            source,
        ));
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "raw-event-strict");
        assert!(event.data_with_context(py, &strict).is_err());
        let allowed = CopyContext::new(CopyPolicy::AllowCopies, "raw-event");
        let snapshot = event.data_with_context(py, &allowed).unwrap();
        assert_eq!(snapshot.as_bytes(), &[3; 16]);
        assert_ne!(snapshot.as_bytes().as_ptr(), pointer);
        let object = Bound::new(py, event).unwrap();
        let view = object.getattr("view").unwrap();
        let buffer = PyUntypedBuffer::get(&view).unwrap();
        assert_eq!(buffer.buf_ptr() as *const u8, pointer);
        assert!(buffer.readonly());
        drop(buffer);
        let slice = view.get_item(PySlice::new(py, 2, 6, 1)).unwrap();
        drop((object, view));
        assert_eq!(budget.usage().owners, 1);
        assert_eq!(
            slice
                .call_method0("tobytes")
                .unwrap()
                .cast::<PyBytes>()
                .unwrap()
                .as_bytes(),
            &[3; 4]
        );
        drop(slice);
        assert_eq!(budget.usage().owners, 0);
        for (context, copied, rejected) in [(&allowed, 16, 0), (&strict, 0, 16)] {
            let report = context.report();
            let c = &report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::VecMaterialization)
                .unwrap()
                .1;
            assert_eq!((c.copied_bytes, c.rejected_bytes), (copied, rejected));
        }
    });
}

#[test]
fn observer_python_pipeline_exports_retained_audio_and_compatible_json_bytes() {
    Python::attach(|py| {
        let budget = BufferBudget::new(1, 8);
        let runtime = budget
            .admit(RuntimeData::Audio {
                samples: vec![0.25, -0.5].into(),
                sample_rate: 48000,
                channels: 1,
                stream_id: Some("source".into()),
                timestamp_us: Some(42),
                arrival_ts_us: None,
                metadata: None,
            })
            .unwrap();
        let RuntimeData::Audio { samples, .. } = &runtime else {
            unreachable!()
        };
        let pointer = samples.as_ptr();
        let expected = serde_json::to_vec(&runtime).unwrap();
        let event = PipelineOutputEvent::from(crate::webrtc::events::PipelineOutputEvent::new(
            "peer".into(),
            runtime,
        ));
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "pipeline-event-strict");
        assert!(event.data_with_context(py, &strict).is_err());
        let allowed = CopyContext::new(CopyPolicy::AllowCopies, "pipeline-event");
        assert_eq!(
            event.data_with_context(py, &allowed).unwrap().as_bytes(),
            expected
        );
        assert_eq!(
            event
                .clone()
                .data_with_context(py, &allowed)
                .unwrap()
                .as_bytes(),
            expected
        );
        let object = Bound::new(py, event).unwrap();
        assert_eq!(
            object
                .getattr("data")
                .unwrap()
                .cast::<PyBytes>()
                .unwrap()
                .as_bytes(),
            expected
        );
        let value = object.getattr("runtime_data").unwrap();
        assert_eq!(
            value
                .get_item("sample_rate")
                .unwrap()
                .extract::<u32>()
                .unwrap(),
            48000
        );
        let samples = value.get_item("samples").unwrap();
        let buffer = PyUntypedBuffer::get(&samples).unwrap();
        assert_eq!(buffer.buf_ptr() as *const f32, pointer);
        assert!(buffer.readonly());
        drop(buffer);
        drop((object, value));
        assert_eq!(budget.usage().owners, 1);
        drop(samples);
        assert_eq!(budget.usage().owners, 0);
        let report = allowed.report();
        assert_eq!(
            report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::PortableSerialization)
                .unwrap()
                .1
                .copied_bytes,
            8
        );
        assert_eq!(
            report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::VecMaterialization)
                .unwrap()
                .1
                .copied_bytes,
            2 * expected.len() as u64
        );
    });
}
