use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy};
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

#[tokio::test]
async fn opus_selected_output_track_uses_core_ingress_budget_through_session_shutdown() {
    use remotemedia_core::manifest::{Manifest, NodeManifest};
    use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor};
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        transport_materialization_policy: CopyPolicy::RequireZeroCopy,
        ingress_retention: Some(remotemedia_traits::buffer_contract::BufferRetention {
            owners: 1,
            bytes: 960 * 4,
        }),
        ..Default::default()
    })
    .unwrap();
    let mut session = executor
        .create_session(Arc::new(Manifest {
            nodes: vec![NodeManifest {
                id: "pass".into(),
                node_type: "PassThrough".into(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap();
    let budget = session.input_sender().unwrap().buffer_budget().unwrap();
    let allocator = session.output_allocator();
    let config = AudioEncoderConfig::default();
    let track = AudioTrack::new(
        Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                ..Default::default()
            },
            "audio".into(),
            "stream".into(),
        )),
        config.clone(),
    )
    .unwrap();
    let packet = AudioEncoder::new(config)
        .unwrap()
        .encode(&vec![0.25; 960])
        .unwrap();
    let data = audio_rtp_to_runtime_data(&packet, &track, allocator.as_ref())
        .await
        .unwrap();
    let RuntimeData::Audio { samples, .. } = &data else {
        panic!("expected audio")
    };
    let pointer = samples.as_ptr();
    session
        .send_input(remotemedia_core::transport::TransportData::new(data))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let RuntimeData::Audio { samples, .. } = output.data else {
        panic!("expected audio")
    };
    assert_eq!(samples.as_ptr(), pointer);
    assert_eq!(budget.usage().owners, 1);
    assert!(track
        .on_rtp_packet_with_allocator(&packet, allocator.as_ref())
        .await
        .is_err());
    let retained = samples.into_shared().slice(1..8).unwrap();
    session.close().await.unwrap();
    session.wait().await.unwrap();
    track.shutdown().await.unwrap();
    assert_eq!(budget.usage().bytes, 960 * 4);
    assert!(retained.iter().all(|sample| sample.is_finite()));
    drop(retained);
    assert_eq!(budget.usage().owners, 0);
    assert!(allocator.allocate_audio(960).is_ok());
}

#[tokio::test]
async fn opus_selected_output_legacy_runtime_adapter_preserves_audio_metadata() {
    let config = AudioEncoderConfig::default();
    let track = AudioTrack::new(
        Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                ..Default::default()
            },
            "audio".into(),
            "stream".into(),
        )),
        config.clone(),
    )
    .unwrap();
    let packet = AudioEncoder::new(config.clone())
        .unwrap()
        .encode(&vec![0.25; 960])
        .unwrap();
    let expected = super::super::audio::AudioDecoder::new(config)
        .unwrap()
        .decode(&packet)
        .unwrap();
    let data = rtp_to_runtime_data(&packet, true, Some(&track), None)
        .await
        .unwrap();
    let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        arrival_ts_us,
        ..
    } = data
    else {
        panic!("expected audio")
    };
    assert_eq!(samples.as_slice(), expected);
    assert_eq!((sample_rate, channels), (48000, 1));
    assert!(arrival_ts_us.is_some_and(|time| time > 0));
    track.shutdown().await.unwrap();
}

#[tokio::test]
async fn audio_owners_real_track_rejects_tail_and_rate_switch_without_policy_downgrade() {
    for channels in [1u16, 2] {
        let track = AudioTrack::new(
            Arc::new(TrackLocalStaticSample::new(
                RTCRtpCodecCapability {
                    mime_type: "audio/opus".into(),
                    ..Default::default()
                },
                "audio".into(),
                "stream".into(),
            )),
            AudioEncoderConfig {
                sample_rate: 48000,
                channels,
                ..Default::default()
            },
        )
        .unwrap();
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "track-strict");
        let frame_size = 960 * channels as usize;
        track
            .send_audio_with_context(vec![0.25; frame_size].into(), 48000, &copies)
            .await
            .unwrap();
        let budget = BufferBudget::new(1, (frame_size * 4) as u64);
        let RuntimeData::Audio { samples, .. } = budget
            .admit(RuntimeData::Audio {
                samples: vec![0.5; frame_size / 2].into(),
                sample_rate: 48000,
                channels: channels as u32,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            })
            .unwrap()
        else {
            unreachable!()
        };
        track
            .send_audio_with_context(samples.into_shared(), 48000, &copies)
            .await
            .unwrap();
        assert_eq!(budget.usage().owners, 1);
        assert!(track
            .finish_audio()
            .await
            .unwrap_err()
            .to_string()
            .contains("track-strict"));
        // Even the legacy API must respect authority attached to the queued tail.
        assert!(track
            .send_audio(Arc::new(vec![0.; 480]), 24000)
            .await
            .unwrap_err()
            .to_string()
            .contains("track-strict"));
        assert_eq!(track.encoder.read().config.sample_rate, 48000);
        assert_eq!(budget.usage().owners, 1);
        tokio::time::timeout(Duration::from_secs(5), track.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.usage().owners, 0);
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.copied_bytes == 0));
    }
}
