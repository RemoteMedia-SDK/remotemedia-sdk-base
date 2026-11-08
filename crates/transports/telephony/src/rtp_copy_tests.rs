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
