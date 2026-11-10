use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy};

#[test]
fn observer_legacy_audio_is_promoted_before_event_fanout() {
    let samples = vec![0.25f32, -0.5];
    let pointer = samples.as_ptr();
    let event = WebRtcEventBridge::pipeline_output(
        "peer".into(),
        RuntimeData::Audio {
            samples: remotemedia_types::AudioSamples::Vec(samples),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        },
        42,
    );
    let clone = event.clone();
    drop(event);
    let WebRtcEventBridge::PipelineOutput {
        data: RuntimeData::Audio { samples, .. },
        ..
    } = clone
    else {
        unreachable!()
    };
    assert!(matches!(
        samples,
        remotemedia_types::AudioSamples::Shared(_)
    ));
    assert_eq!(samples.as_ptr(), pointer);
    assert_eq!(samples.as_slice(), &[0.25, -0.5]);
}

#[test]
fn observer_events_and_queue_clones_retain_original_buffers() {
    for pipeline in [false, true] {
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "observer-queue");
        let budget = BufferBudget::new(1, 16);
        let RuntimeData::Binary(source) = budget
            .admit(RuntimeData::Binary(vec![7; 16].into()))
            .unwrap()
        else {
            unreachable!()
        };
        let pointer = source.as_ptr();
        let event = if pipeline {
            WebRtcEventBridge::pipeline_output_with_context(
                "peer".into(),
                RuntimeData::Binary(source),
                42,
                &copies,
            )
        } else {
            WebRtcEventBridge::data_received_with_context("peer".into(), source, 42, &copies)
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(event.clone()).unwrap();
        drop((event, tx));
        let data = match rx.try_recv().unwrap() {
            WebRtcEventBridge::PipelineOutput {
                data: RuntimeData::Binary(data),
                timestamp_ns: 42,
                copies: received_copies,
                ..
            }
            | WebRtcEventBridge::DataReceived {
                data,
                timestamp_ns: 42,
                copies: received_copies,
                ..
            } => {
                assert!(copies.same_authority(&received_copies));
                data
            }
            _ => unreachable!(),
        };
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(&*data, &[7; 16]);
        drop((data, rx));
        assert_eq!(budget.usage().owners, 0);
    }
}
