use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy, RuntimeData};

fn admitted(budget: &BufferBudget, values: Vec<f32>) -> SharedBuffer<f32> {
    let RuntimeData::Audio { samples, .. } = budget
        .admit(RuntimeData::Audio {
            samples: values.into(),
            sample_rate: 48000,
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
    samples.into_shared()
}

#[test]
fn audio_owners_adjacent_strict_views_retain_identity_and_final_credit() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "joined-pcm");
    let budget = BufferBudget::new(1, 32);
    let source = admitted(&budget, vec![0.25; 8]);
    let pointer = source.as_ptr();
    let mut packetizer = AudioPcmPacketizer::new(4);
    packetizer
        .append(source.slice(0..2).unwrap(), &copies)
        .unwrap();
    assert!(packetizer.next_frame().unwrap().is_none());
    packetizer
        .append(source.slice(2..8).unwrap(), &copies)
        .unwrap();
    drop(source);
    let first = packetizer.next_frame().unwrap().unwrap();
    let second = packetizer.next_frame().unwrap().unwrap();
    assert_eq!(first.as_ptr(), pointer);
    assert_eq!(second.as_ptr() as usize, pointer as usize + 4 * 4);
    assert!(packetizer.finish().unwrap().is_none());
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0 && c.rejected_bytes == 0));
    drop((first, packetizer));
    assert_eq!(budget.usage().owners, 1);
    drop(second);
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn audio_owners_assembly_and_tail_count_only_transferred_samples() {
    let a = CopyContext::new(CopyPolicy::AllowCopies, "first-owner");
    let b = CopyContext::new(CopyPolicy::AllowCopies, "second-owner");
    let mut packetizer = AudioPcmPacketizer::new(4);
    packetizer.append(vec![1., 2., 3.].into(), &a).unwrap();
    packetizer.append(vec![4., 5., 6.].into(), &b).unwrap();
    assert_eq!(
        &*packetizer.next_frame().unwrap().unwrap(),
        &[1., 2., 3., 4.]
    );
    let (tail, real) = packetizer.finish().unwrap().unwrap();
    assert_eq!(real, 2);
    assert_eq!(&*tail, &[5., 6., 0., 0.]);
    for copies in [a, b] {
        let report = copies.report();
        let counts = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::VecMaterialization)
            .unwrap()
            .1;
        assert_eq!(
            (counts.copied_bytes, counts.failed, counts.rejected),
            (12, 0, 0)
        );
    }
}

#[test]
fn audio_owners_strict_fragment_rejects_before_any_copy_and_clear_releases() {
    for strict_first in [false, true] {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-fragment");
        let allow = CopyContext::new(CopyPolicy::AllowCopies, "allowed-fragment");
        let budget = BufferBudget::new(2, 16);
        let mut packetizer = AudioPcmPacketizer::new(4);
        packetizer
            .append(
                admitted(&budget, vec![1.; 2]),
                if strict_first { &strict } else { &allow },
            )
            .unwrap();
        packetizer
            .append(
                admitted(&budget, vec![2.; 2]),
                if strict_first { &allow } else { &strict },
            )
            .unwrap();
        assert!(packetizer
            .next_frame()
            .unwrap_err()
            .to_string()
            .contains("strict-fragment"));
        assert_eq!(packetizer.queued_samples(), 4);
        assert_eq!(budget.usage().owners, 2);
        assert!(allow
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.copied_bytes == 0 && c.failed == 0));
        assert!(packetizer.finish().is_err());
        packetizer.clear();
        assert_eq!(budget.usage().owners, 0);
        assert!(packetizer.finish().unwrap().is_none());
    }
}

#[test]
fn audio_owners_opus_matches_contiguous_reference_for_mono_and_stereo() {
    use crate::media::audio::{AudioDecoder, AudioEncoder, AudioEncoderConfig};
    for channels in [1u16, 2] {
        let config = AudioEncoderConfig {
            sample_rate: 48000,
            channels,
            ..Default::default()
        };
        let frame_size = 960 * channels as usize;
        let source: SharedBuffer<f32> = (0..frame_size * 2)
            .map(|i| (i as f32 * 0.01).sin() * 0.25)
            .collect::<Vec<_>>()
            .into();
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "opus-source");
        let mut packetizer = AudioPcmPacketizer::new(frame_size);
        let mut encoder = AudioEncoder::new(config.clone()).unwrap();
        let mut reference = AudioEncoder::new(config.clone()).unwrap();
        let mut decoder = AudioDecoder::new(config).unwrap();
        packetizer
            .append(source.slice(0..17).unwrap(), &copies)
            .unwrap();
        packetizer
            .append(source.slice(17..source.len()).unwrap(), &copies)
            .unwrap();
        for index in 0..2 {
            let frame = packetizer.next_frame().unwrap().unwrap();
            let encoded = encoder.encode(&frame).unwrap();
            assert_eq!(
                encoded,
                reference
                    .encode(&source[index * frame_size..(index + 1) * frame_size])
                    .unwrap()
            );
            assert_eq!(decoder.decode(&encoded).unwrap().len(), frame_size);
        }
        assert!(packetizer.finish().unwrap().is_none());
    }
}
