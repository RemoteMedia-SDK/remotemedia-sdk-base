use super::*;
use ac_ffmpeg::packet::PacketMut;
use remotemedia_types::CopyPolicy;

fn packet(bytes: &[u8], key: bool) -> Packet {
    let mut packet = PacketMut::new(bytes.len()).with_key_flag(key);
    packet.data_mut().copy_from_slice(bytes);
    packet.freeze()
}

#[test]
fn codec_packets_single_owner_retains_pointer_and_nested_views_across_threads() {
    let source = packet(b"encoded bytes", true);
    let pointer = source.data().as_ptr();
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "packet-output");
    let (output, key) = publish_packets(vec![source], &copies).unwrap();
    assert!(key);
    assert_eq!(output.as_ptr(), pointer);
    assert_eq!(
        output.owner::<PacketOwner>().unwrap().0.data().as_ptr(),
        pointer
    );
    let retained = output.slice(1..12).unwrap().slice(1..5).unwrap();
    assert!(output.shares_storage(&retained));
    assert_eq!(retained.as_ptr(), pointer.wrapping_add(2));
    drop(output);
    std::thread::spawn(move || assert_eq!(retained.as_slice(), b"code"))
        .join()
        .unwrap();
    assert!(copies.report().entries.iter().all(|(_, entry)| {
        entry.completed == 0 && entry.failed == 0 && entry.rejected_bytes == 0
    }));
}

#[test]
fn codec_packets_last_view_releases_ffmpeg_reference() {
    let source = packet(b"retained", false);
    let alias = source.clone();
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "packet-release");
    let (output, _) = publish_packets(vec![source], &copies).unwrap();
    let retained = output.slice(1..3).unwrap();
    drop(output);
    let alias = alias
        .try_into_mut()
        .err()
        .expect("view must retain the FFmpeg buffer");
    assert_eq!(retained.as_slice(), b"et");
    drop(retained);
    assert!(
        alias.try_into_mut().is_ok(),
        "last view must release its FFmpeg buffer reference"
    );
}

#[test]
fn codec_packets_decoder_reuses_complete_owners_but_checks_partial_ranges() {
    let source = packet(b"packet bytes", true);
    let pointer = source.data().as_ptr();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "packet-ingress");
    let (output, _) = publish_packets(vec![source], &strict).unwrap();
    let decoded = decoder_packet(output.slice(0..output.len()).unwrap(), &strict).unwrap();
    assert_eq!(decoded.data().as_ptr(), pointer);
    let partial = output.slice(1..5).unwrap();
    assert!(decoder_packet(partial.clone(), &strict).is_err());
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "packet-staging");
    let staged = decoder_packet(partial, &copies).unwrap();
    assert_eq!(staged.data(), b"acke");
    assert_ne!(staged.data().as_ptr(), pointer.wrapping_add(1));
    drop(output);
    assert_eq!(decoded.data(), b"packet bytes");
    let counts = copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1;
    assert_eq!((counts.completed, counts.copied_bytes), (1, 4));
}

#[test]
fn codec_packets_decoder_rejects_unowned_bytes_before_staging() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "unowned-packet");
    assert!(decoder_packet(vec![1, 2, 3].into(), &strict).is_err());
    let counts = strict
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1;
    assert_eq!((counts.rejected_bytes, counts.copied_bytes), (3, 0));
}

#[test]
fn codec_packets_empty_outputs_and_flags_do_not_force_payload_copies() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "empty-packets");
    assert!(publish_packets(vec![], &copies).unwrap().0.is_empty());
    let (empty, key) = publish_packets(vec![packet(b"", true)], &copies).unwrap();
    assert!(empty.is_empty() && key);
    let source = packet(b"one", false);
    let pointer = source.data().as_ptr();
    let (output, key) = publish_packets(vec![packet(b"", true), source], &copies).unwrap();
    assert_eq!(output.as_ptr(), pointer);
    assert!(key);
}

#[test]
fn codec_packets_aggregation_is_exact_and_rejected_under_strict_policy() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let copies = CopyContext::new(policy, "packet-aggregation");
        let result = publish_packets(vec![packet(b"abc", false), packet(b"de", true)], &copies);
        let counts = copies
            .report()
            .entries
            .into_iter()
            .find(|(reason, _)| *reason == CopyReason::VecMaterialization)
            .unwrap()
            .1;
        if policy == CopyPolicy::AllowCopies {
            let (output, key) = result.unwrap();
            assert_eq!(output.as_slice(), b"abcde");
            assert!(key);
            assert_eq!((counts.completed, counts.copied_bytes), (1, 5));
        } else {
            assert!(result.is_err());
            assert_eq!((counts.rejected_bytes, counts.copied_bytes), (5, 0));
        }
        assert_eq!(counts.in_flight, 0);
    }
}

#[test]
fn codec_packets_real_h264_encoder_and_decoder_share_packet_storage() {
    use super::super::codec::{
        FFmpegDecoder, FFmpegEncoder, VideoDecoderBackend, VideoEncoderBackend,
    };
    use super::super::decoder::VideoDecoderConfig;
    use super::super::encoder::VideoEncoderConfig;
    use crate::data::{
        video::{PixelFormat, VideoCodec},
        RuntimeData,
    };
    let mut encoder = FFmpegEncoder::new(VideoEncoderConfig {
        codec: VideoCodec::H264,
        hardware_accel: false,
        ..Default::default()
    })
    .unwrap();
    let output = encoder
        .encode(RuntimeData::Video {
            pixel_data: vec![128; 32 * 32 * 3 / 2].into(),
            width: 32,
            height: 32,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 7,
            timestamp_us: 123,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        })
        .unwrap();
    let decode_input = output.clone();
    let RuntimeData::Video {
        pixel_data,
        codec,
        frame_number,
        timestamp_us,
        ..
    } = output
    else {
        panic!("encoder did not return video");
    };
    assert_eq!(
        (codec, frame_number, timestamp_us),
        (Some(VideoCodec::H264), 7, 123)
    );
    assert!(!pixel_data.is_empty());
    assert!(pixel_data.owner::<PacketOwner>().is_some());
    let retained = pixel_data.slice(0..pixel_data.len()).unwrap();
    let expected = retained.as_slice().to_vec(); // Fixture snapshot, outside the implementation.
    drop((pixel_data, encoder));
    assert_eq!(retained.as_slice(), expected);
    let before = CopyContext::legacy()
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1
        .copied_bytes;
    let mut decoder = FFmpegDecoder::new(VideoDecoderConfig {
        expected_codec: Some(VideoCodec::H264),
        ..Default::default()
    })
    .unwrap();
    let decoded = decoder.decode(decode_input).unwrap();
    assert!(matches!(
        decoded,
        RuntimeData::Video {
            width: 32,
            height: 32,
            codec: None,
            ..
        }
    ));
    let after = CopyContext::legacy()
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1
        .copied_bytes;
    assert_eq!(
        after - before,
        32 * 32 * 3 / 2,
        "owning codec chain transfers only visible decoded pixels, not packet ingress"
    );
}
