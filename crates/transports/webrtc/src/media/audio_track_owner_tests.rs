use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy};
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

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
