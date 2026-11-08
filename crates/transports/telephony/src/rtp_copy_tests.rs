use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy};

fn counts(copies: &CopyContext, reason: CopyReason) -> remotemedia_types::CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[test]
fn rtp_copy_owned_payload_and_jitter_clone_keep_the_datagram_reservation() {
    let wire = RtpPacketizer::new(0, 1, 1, 0, 160)
        .unwrap()
        .packetize(vec![0xff; 160])
        .write();
    let pointer = wire.as_ptr();
    let budget = BufferBudget::new(1, 172);
    let RuntimeData::Binary(wire) = budget.admit(RuntimeData::Binary(wire.into())).unwrap() else {
        unreachable!()
    };
    let packet = RtpPacket::parse_owned(wire).unwrap();
    assert_eq!(packet.payload.as_ptr(), pointer.wrapping_add(12));
    let mut jitter = JitterBuffer::new(2);
    assert_eq!(jitter.push(packet), JitterDecision::Accepted);
    let mut other = jitter.clone();
    drop(jitter);
    assert_eq!(budget.usage().bytes, 172);
    let retained = other.pop_ready().unwrap().payload.slice(7..11).unwrap();
    drop(other);
    assert_eq!(retained.as_ptr(), pointer.wrapping_add(19));
    std::thread::spawn(move || assert_eq!(retained.as_slice(), &[0xff; 4]))
        .join()
        .unwrap();
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn rtp_copy_borrowed_receive_and_final_wire_use_explicit_authority() {
    let packet = RtpPacketizer::new(0, 1, 1, 0, 160)
        .unwrap()
        .packetize(vec![0xff; 160]);
    let allowed = CopyContext::new(CopyPolicy::AllowCopies, "rtp-wire");
    let wire = packet.write_with_context(&allowed).unwrap();
    assert_eq!(wire.len(), 172);
    assert_eq!(
        counts(&allowed, CopyReason::PortableSerialization).copied_bytes,
        160
    );
    let parsed = RtpPacket::parse_with_context(&wire, &allowed).unwrap();
    assert_eq!(parsed, packet);
    assert_eq!(
        counts(&allowed, CopyReason::BorrowedInput).copied_bytes,
        160
    );
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "rtp-strict");
    assert!(packet
        .write_with_context(&strict)
        .unwrap_err()
        .to_string()
        .contains("rtp-strict"));
    assert!(RtpPacket::parse_with_context(&wire, &strict)
        .unwrap_err()
        .to_string()
        .contains("rtp-strict"));
    assert_eq!(
        counts(&strict, CopyReason::PortableSerialization).rejected_bytes,
        160
    );
    assert_eq!(
        counts(&strict, CopyReason::BorrowedInput).rejected_bytes,
        160
    );
    for end in 0..12 {
        assert!(RtpPacket::parse_with_context(&wire[..end], &strict).is_err());
    }
    assert_eq!(counts(&strict, CopyReason::BorrowedInput).rejected, 1);
    // Header-only packets carry no payload; strict policy permits metadata.
    let empty = RtpPacket::parse_with_context(&wire[..12], &strict).unwrap();
    assert_eq!(empty.write_with_context(&strict).unwrap().len(), 12);
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}

#[tokio::test]
async fn rtp_copy_udp_owned_ingress_computes_g711_and_opus_under_strict_policy() {
    let tx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rx = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for (codec, pt, rate) in [
        (AudioCodec::Pcmu, 0, 8000),
        (AudioCodec::Pcma, 8, 8000),
        (AudioCodec::Opus, 111, 48000),
    ] {
        let send = CopyContext::new(CopyPolicy::AllowCopies, "rtp-send");
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "rtp-receive");
        let mut outbound = RtpOutboundMediaSession::new(codec, pt, 1, 1, 0, 20)
            .unwrap()
            .with_copy_context(send.clone());
        let wire = outbound
            .packetize_audio_datagram(&vec![0.0; rate / 50])
            .unwrap();
        assert_eq!(
            counts(&send, CopyReason::PortableSerialization).copied_bytes,
            (wire.len() - 12) as u64
        );
        tx.send_to(&wire, rx.local_addr().unwrap()).await.unwrap();
        let mut received = vec![0; 1500];
        let pointer = received.as_ptr();
        let (len, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx.recv_from(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        received.truncate(len);
        let owned: SharedBytes = received.into();
        assert_eq!(owned.as_ptr(), pointer);
        let mut codecs = CodecMap::default();
        codecs.insert(pt, codec).unwrap();
        let mut inbound = RtpMediaSession::new(
            "call".into(),
            "leg".into(),
            ParticipantRole::User,
            codecs,
            2,
        )
        .with_copy_context(strict.clone());
        let frames = inbound.receive_owned_datagram(owned).unwrap();
        assert_eq!(frames.len(), 1);
        let RuntimeData::Audio {
            samples,
            sample_rate,
            ..
        } = &frames[0].data
        else {
            unreachable!()
        };
        assert_eq!(*sample_rate, rate as u32);
        assert_eq!(samples.len(), rate / 50);
        assert!(samples.iter().all(|sample| sample.is_finite()));
        assert!(strict
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.copied_bytes == 0 && c.rejected == 0));
        let mut rejected = RtpOutboundMediaSession::new(codec, pt, 1, 1, 0, 20)
            .unwrap()
            .with_copy_context(strict.clone());
        assert!(rejected
            .packetize_audio_datagram(&vec![0.0; rate / 50])
            .unwrap_err()
            .to_string()
            .contains("rtp-receive"));
        assert_eq!(
            counts(&strict, CopyReason::PortableSerialization).rejected,
            1
        );
    }
}

#[test]
fn pcm_owned_frames_match_all_codecs_and_retain_credit_without_staging() {
    for (codec, rate) in [
        (AudioCodec::Pcmu, 8000),
        (AudioCodec::Pcma, 8000),
        (AudioCodec::Opus, 48000),
    ] {
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "pcm-owned");
        let size = rate as usize / 50;
        let values: Vec<f32> = (0..size * 2)
            .map(|i| (i as f32 * 0.01).sin() * 0.25)
            .collect();
        let pointer = values.as_ptr();
        let budget = BufferBudget::new(1, (size * 8) as u64);
        let RuntimeData::Audio { samples, .. } = budget
            .admit(RuntimeData::Audio {
                samples: values.into(),
                sample_rate: rate,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            })
            .unwrap()
        else {
            unreachable!()
        };
        let source = samples.into_shared();
        let mut session = RtpOutboundMediaSession::new(codec, 0, 1, 1, 0, 20)
            .unwrap()
            .with_copy_context(copies.clone());
        let mut reference = RtpOutboundMediaSession::new(codec, 0, 1, 1, 0, 20).unwrap();
        session
            .queue_audio(source.slice(0..17).unwrap(), rate, 1)
            .unwrap();
        session
            .queue_audio(source.slice(17..size * 2).unwrap(), rate, 1)
            .unwrap();
        let first = session.pcm.next_frame().unwrap().unwrap();
        assert_eq!(first.as_ptr(), pointer);
        assert_eq!(
            session.packetize_audio_frame(&first).unwrap(),
            reference.packetize_audio_frame(&source[..size]).unwrap()
        );
        assert_eq!(
            session.next_audio_packet().unwrap().unwrap(),
            reference.packetize_audio_frame(&source[size..]).unwrap()
        );
        drop(source);
        assert_eq!(budget.usage().owners, 1);
        drop(first);
        assert_eq!(budget.usage().owners, 0);
        assert!(session.next_audio_packet().unwrap().is_none());
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, count)| count.copied_bytes == 0 && count.rejected_bytes == 0));
    }
}

#[test]
fn pcm_fragment_assembly_is_counted_and_strict_failure_keeps_queue_until_clear() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let copies = CopyContext::new(policy, "pcm-fragments");
        let mut session = RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 1, 1, 0, 20)
            .unwrap()
            .with_copy_context(copies.clone());
        let budget = BufferBudget::new(2, 640);
        for value in [0.1, 0.2] {
            let RuntimeData::Audio { samples, .. } = budget
                .admit(RuntimeData::Audio {
                    samples: vec![value; 80].into(),
                    sample_rate: 8000,
                    channels: 1,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: None,
                })
                .unwrap()
            else {
                unreachable!()
            };
            session.queue_audio(samples.into_shared(), 8000, 1).unwrap();
        }
        if policy == CopyPolicy::AllowCopies {
            let wire = session.next_audio_datagram().unwrap().unwrap();
            assert_eq!(wire.len(), 172);
            assert_eq!(
                counts(&copies, CopyReason::VecMaterialization).copied_bytes,
                640
            );
            assert_eq!(
                counts(&copies, CopyReason::PortableSerialization).copied_bytes,
                160
            );
            assert_eq!(budget.usage().owners, 0);
        } else {
            assert!(session
                .next_audio_packet()
                .unwrap_err()
                .to_string()
                .contains("pcm-fragments"));
            assert_eq!(session.pcm.queued_samples(), 160);
            assert_eq!(budget.usage().owners, 2);
            assert_eq!(
                counts(&copies, CopyReason::VecMaterialization).copied_bytes,
                0
            );
            session.clear_pending_audio();
            assert_eq!(budget.usage().owners, 0);
        }
    }
}

#[test]
fn pcm_conversion_averages_channels_and_accounts_mono_resampling() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "pcm-convert");
    let stereo: SharedBuffer<f32> = vec![1., 3., 2., 4., 5., 7., 6., 8.].into();
    assert_eq!(
        &*prepare_pcm(stereo.clone(), 8000, 2, 8000, &strict).unwrap(),
        &[2., 3., 6., 7.]
    );
    assert_eq!(
        &*prepare_pcm(stereo, 16000, 2, 8000, &strict).unwrap(),
        &[2., 6.]
    );
    let mono: SharedBuffer<f32> = vec![1., 2., 3., 4.].into();
    assert!(prepare_pcm(mono.clone(), 16000, 1, 8000, &strict).is_err());
    assert_eq!(
        counts(&strict, CopyReason::VecMaterialization).rejected_bytes,
        8
    );
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "pcm-resample");
    assert_eq!(
        &*prepare_pcm(mono.clone(), 16000, 1, 8000, &allow).unwrap(),
        &[1., 3.]
    );
    assert_eq!(
        &*prepare_pcm(mono, 4000, 1, 8000, &allow).unwrap(),
        &[1., 1., 2., 2., 3., 3., 4., 4.]
    );
    assert_eq!(
        counts(&allow, CopyReason::VecMaterialization).copied_bytes,
        40
    );
}

#[test]
fn pcm_invalid_input_preserves_queue_and_incomplete_tail_can_be_discarded() {
    assert!(RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 1, 1, 0, 0).is_err());
    let mut session = RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 1, 1, 0, 20).unwrap();
    session.queue_audio(vec![0.; 80].into(), 8000, 1).unwrap();
    for (rate, channels) in [(0, 1), (8000, 0), (8000, 2)] {
        assert!(session
            .queue_audio(vec![0.; 3].into(), rate, channels)
            .is_err());
        assert_eq!(session.pcm.queued_samples(), 80);
    }
    assert!(session.next_audio_packet().unwrap().is_none());
    session.clear_pending_audio();
    assert_eq!(session.pcm.queued_samples(), 0);
}
