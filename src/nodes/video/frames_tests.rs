use super::*;
use crate::data::RuntimeData;
use crate::nodes::video::{VideoEncoderConfig, VideoEncoderNode};
use crate::nodes::{AsyncStreamingNode, NodeRuntimeContext};
use remotemedia_types::CopyPolicy;

#[test]
fn codec_frames_conversion_matches_committed_y4m_baseline() {
    // Frozen pre-refactor SDK video_sink_golden.jsonl; independent expected bytes.
    for line in include_str!("frames_golden.jsonl").lines() {
        let (w, h, expected): (usize, usize, Vec<u8>) = serde_json::from_str(line).unwrap();
        let rgb: Vec<u8> = (0..w * h * 3)
            .map(|i| ((i * 73 + 29) % 256) as u8)
            .collect();
        assert_eq!(crate::data::video::rgb24_to_yuv420p(&rgb, w, h), expected);
        let rgba: Vec<u8> = rgb
            .chunks_exact(3)
            .flat_map(|p| [p[0], p[1], p[2], 23])
            .collect();
        assert_eq!(crate::data::video::rgba32_to_yuv420p(&rgba, w, h), expected);
    }
}

#[test]
fn codec_frames_rgb_and_rgba_match_golden_odd_layout_and_preserve_padding() {
    let rgb = [
        255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 255, 0, 0, 0, 0, 255, 255, 255,
        255, 0, 255, 0,
    ];
    let golden = [
        76, 150, 29, 255, 0, 76, 29, 255, 150, 96, 170, 192, 44, 133, 181, 118, 21,
    ];
    assert_eq!(crate::data::video::rgb24_to_yuv420p(&rgb, 3, 3), golden);
    let rgba: Vec<_> = rgb
        .chunks_exact(3)
        .flat_map(|pixel| [pixel[0], pixel[1], pixel[2], 17])
        .collect();
    assert_eq!(crate::data::video::rgba32_to_yuv420p(&rgba, 3, 3), golden);
    for (input, channels) in [(rgb.as_slice(), 3), (rgba.as_slice(), 4)] {
        let (mut y, mut u, mut v) = ([9; 15], [9; 8], [9; 10]);
        rgb_to_yuv420p_planes(
            input,
            3,
            3,
            channels,
            [(&mut y, 5), (&mut u, 4), (&mut v, 5)],
        )
        .unwrap();
        assert_eq!(y, [76, 150, 29, 9, 9, 255, 0, 76, 9, 9, 29, 255, 150, 9, 9]);
        assert_eq!(u, [96, 170, 9, 9, 192, 44, 9, 9]);
        assert_eq!(v, [133, 181, 9, 9, 9, 118, 21, 9, 9, 9]);
    }
}

#[test]
fn codec_frames_invalid_planes_fail_before_any_write() {
    for bad_stride in [0, 1, usize::MAX] {
        let (mut y, mut u, mut v) = ([9; 16], [9; 4], [9; 4]);
        assert!(rgb_to_yuv420p_planes(
            &[123; 48],
            4,
            4,
            3,
            [(&mut y, 4), (&mut u, 2), (&mut v, bad_stride)]
        )
        .is_err());
        assert_eq!((y, u, v), ([9; 16], [9; 4], [9; 4]));
    }
    let (mut y, mut u, mut v) = ([9; 16], [9; 4], [9; 3]);
    assert!(
        rgb_to_yuv420p_planes(&[123; 48], 4, 4, 3, [(&mut y, 4), (&mut u, 2), (&mut v, 2)])
            .is_err()
    );
    assert_eq!((y, u, v), ([9; 16], [9; 4], [9; 3]));
    for (w, h, channels, len) in [
        (0, 4, 3, 0),
        (usize::MAX, 2, 3, 0),
        (4, 4, 2, 32),
        (4, 4, 3, 47),
    ] {
        let (mut y, mut u, mut v) = ([9; 16], [9; 4], [9; 4]);
        assert!(rgb_to_yuv420p_planes(
            &vec![0; len],
            w,
            h,
            channels,
            [(&mut y, 4), (&mut u, 2), (&mut v, 2)]
        )
        .is_err());
        assert_eq!((y, u, v), ([9; 16], [9; 4], [9; 4]));
    }
}

#[test]
fn codec_frames_yuv_rows_exclude_padding_and_count_visible_bytes() {
    let (w, h) = (34, 18);
    let input: Vec<_> = (0..w * h * 3 / 2).map(|i| (i % 251) as u8).collect();
    for format in [PixelFormat::Yuv420p, PixelFormat::I420] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "yuv-frame");
        let frame = encoder_frame(&input, w as u32, h as u32, format, &copies)
            .unwrap()
            .freeze();
        assert!(
            frame.planes()[0].line_size() > w,
            "fixture must exercise padded native rows"
        );
        let mut offset = 0;
        for (plane, (columns, rows)) in
            frame
                .planes()
                .iter()
                .take(3)
                .zip([(w, h), (w / 2, h / 2), (w / 2, h / 2)])
        {
            for row in 0..rows {
                assert_eq!(
                    &plane.line(row).unwrap()[..columns],
                    &input[offset..offset + columns]
                );
                offset += columns;
            }
        }
        assert_eq!(offset, input.len());
        let counts = copies
            .report()
            .entries
            .into_iter()
            .find(|(r, _)| *r == CopyReason::ForeignPoolIngress)
            .unwrap()
            .1;
        assert_eq!(
            (counts.completed, counts.copied_bytes),
            (1, input.len() as u64)
        );
    }
}

#[test]
fn codec_frames_rgb_computes_directly_under_strict_policy() {
    for channels in [3, 4] {
        let input: Vec<_> = (0..34 * 18)
            .flat_map(|_| [255, 0, 0, 83].into_iter().take(channels))
            .collect();
        let format = if channels == 3 {
            PixelFormat::Rgb24
        } else {
            PixelFormat::Rgba32
        };
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "rgb-frame");
        let frame = encoder_frame(&input, 34, 18, format, &copies)
            .unwrap()
            .freeze();
        for (plane, (columns, rows, value)) in
            frame
                .planes()
                .iter()
                .take(3)
                .zip([(34, 18, 76), (17, 9, 85), (17, 9, 255)])
        {
            for row in 0..rows {
                assert!(plane.line(row).unwrap()[..columns]
                    .iter()
                    .all(|&b| b == value));
            }
        }
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.copied_bytes == 0 && c.rejected_bytes == 0));
    }
}

#[test]
fn codec_frames_strict_yuv_and_invalid_geometry_reject_before_staging() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-frame");
    assert!(matches!(
        encoder_frame(&[128; 24], 4, 4, PixelFormat::Yuv420p, &copies),
        Err(CodecError::CopyRejected(_))
    ));
    for (w, h, format) in [
        (0, 4, PixelFormat::Yuv420p),
        (3, 4, PixelFormat::Rgb24),
        (u32::MAX, 4, PixelFormat::Rgb24),
        (4, 4, PixelFormat::NV12),
        (4, 4, PixelFormat::Yuv420p),
    ] {
        assert!(matches!(
            encoder_frame(&[], w, h, format, &copies),
            Err(CodecError::InvalidInput(_))
        ));
    }
    let counts = copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1;
    assert_eq!(
        (counts.rejected_bytes, counts.completed, counts.failed),
        (24, 0, 0)
    );
}

fn input_frame(format: PixelFormat) -> RuntimeData {
    RuntimeData::Video {
        pixel_data: vec![
            128;
            if format == PixelFormat::Rgb24 {
                34 * 18 * 3
            } else {
                34 * 18 * 3 / 2
            }
        ]
        .into(),
        width: 34,
        height: 18,
        format,
        codec: None,
        frame_number: 7,
        timestamp_us: 123,
        is_keyframe: false,
        stream_id: None,
        arrival_ts_us: None,
    }
}

#[tokio::test]
async fn codec_frames_node_hooks_retain_policy_and_recover_after_rejection() {
    use std::sync::Arc;
    let node = VideoEncoderNode::new(VideoEncoderConfig {
        codec: crate::data::video::VideoCodec::H264,
        hardware_accel: false,
        ..Default::default()
    })
    .unwrap();
    let mut ctx = NodeRuntimeContext::for_session(
        "frames",
        "encoder",
        crate::transport::session_control::SessionControl::new("frames"),
        Arc::new(()),
    );
    ctx.materialization = CopyContext::new(CopyPolicy::RequireZeroCopy, "frame-node");
    assert!(node
        .process_with_ctx(input_frame(PixelFormat::Yuv420p), &ctx)
        .await
        .is_err());
    assert!(node
        .process_multi_with_ctx(
            std::collections::HashMap::from([("in".into(), input_frame(PixelFormat::Yuv420p))]),
            &ctx
        )
        .await
        .is_err());
    let mut called = false;
    assert!(node
        .process_streaming_with_ctx(input_frame(PixelFormat::Yuv420p), &ctx, |_| {
            called = true;
            Ok(())
        })
        .await
        .is_err());
    assert!(!called);
    let counts = ctx
        .materialization
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1;
    assert_eq!(
        (counts.rejected_bytes, counts.copied_bytes),
        (3 * 34 * 18 * 3 / 2, 0)
    );
    let output = node
        .process_with_ctx(input_frame(PixelFormat::Rgb24), &ctx)
        .await
        .unwrap();
    assert!(matches!(
        output,
        RuntimeData::Video {
            width: 34,
            height: 18,
            frame_number: 7,
            ..
        }
    ));
    assert!(ctx
        .materialization
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}
