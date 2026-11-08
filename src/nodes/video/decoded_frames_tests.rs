use super::*;
use crate::data::{video::VideoCodec, RuntimeData};
use crate::nodes::video::codec::{FFmpegEncoder, VideoEncoderBackend};
use crate::nodes::video::{VideoDecoderConfig, VideoDecoderNode, VideoEncoderConfig};
use crate::nodes::{AsyncStreamingNode, NodeRuntimeContext};
use remotemedia_types::{BufferAllocationError, BufferLoan, CopyPolicy, HeapOutputAllocator};
use std::sync::Arc;

fn native_frame() -> (VideoFrame, Vec<u8>) {
    let mut bytes = vec![81; 34 * 18];
    bytes.extend(vec![90; 17 * 9]);
    bytes.extend(vec![240; 17 * 9]);
    let frame = super::super::frames::encoder_frame(
        &bytes,
        34,
        18,
        PixelFormat::Yuv420p,
        &CopyContext::new(CopyPolicy::AllowCopies, "fixture"),
    )
    .unwrap()
    .freeze();
    (frame, bytes)
}

fn counts(copies: &CopyContext) -> remotemedia_types::CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1
}

#[test]
fn codec_decoded_layout_checks_dimensions_and_formats() {
    for (w, h, format) in [
        (0, 2, PixelFormat::Rgb24),
        (2, 0, PixelFormat::Yuv420p),
        (usize::MAX, 2, PixelFormat::Rgba32),
        (3, 4, PixelFormat::Yuv420p),
        (4, 3, PixelFormat::NV12),
        (4, 4, PixelFormat::Encoded),
        (4, 4, PixelFormat::Unspecified),
    ] {
        assert!(matches!(
            Layout::new(w, h, format),
            Err(CodecError::OutputLayout(_))
        ));
    }
    assert_eq!(Layout::new(3, 5, PixelFormat::Rgba32).unwrap().bytes, 60);
}

#[test]
fn codec_decoded_planar_rows_omit_native_padding() {
    let (frame, mut expected) = native_frame();
    // Independently varying rows catch a contiguous-prefix copy disguised as packing.
    for (i, byte) in expected.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    let frame = super::super::frames::encoder_frame(
        &expected,
        34,
        18,
        PixelFormat::I420,
        &CopyContext::new(CopyPolicy::AllowCopies, "fixture"),
    )
    .unwrap()
    .with_time_base(frame.time_base())
    .freeze();
    assert!(frame.planes()[0].line_size() > 34);
    for format in [PixelFormat::Yuv420p, PixelFormat::I420] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "packed-frame");
        let output = decoder_frame(&frame, format, &copies, &HeapOutputAllocator).unwrap();
        assert_eq!(output.as_slice(), expected);
        assert_eq!(
            (counts(&copies).completed, counts(&copies).copied_bytes),
            (1, 918)
        );
    }
}

#[test]
fn codec_decoded_converts_to_actual_requested_pixel_layout() {
    let (frame, _) = native_frame();
    for format in [PixelFormat::NV12, PixelFormat::Rgb24, PixelFormat::Rgba32] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "converted-frame");
        let output = decoder_frame(&frame, format, &copies, &HeapOutputAllocator).unwrap();
        if format == PixelFormat::NV12 {
            assert_eq!(output.len(), 918);
            assert!(output[..612].iter().all(|&b| b == 81));
            assert!(output[612..].chunks_exact(2).all(|uv| uv == [90, 240]));
        } else {
            let channels = if format == PixelFormat::Rgb24 { 3 } else { 4 };
            assert_eq!(output.len(), 612 * channels);
            // BT.601 limited-range red; allow integer rounding differences in swscale.
            for pixel in output.chunks_exact(channels) {
                assert!(
                    pixel[0] >= 250 && pixel[1] <= 3 && pixel[2] <= 3,
                    "{pixel:?}"
                );
                if channels == 4 {
                    assert_eq!(pixel[3], 255);
                }
            }
        }
        assert_eq!(counts(&copies).copied_bytes, output.len() as u64);
    }
}

#[test]
fn codec_decoded_accepts_native_rgb_instead_of_assuming_three_yuv_planes() {
    use ac_ffmpeg::codec::video::VideoFrameMut;
    let mut frame = VideoFrameMut::black(get_pixel_format("rgb24"), 3, 5);
    for row in 0..5 {
        for pixel in frame.planes_mut()[0].line_mut(row).unwrap()[..9].chunks_exact_mut(3) {
            pixel.copy_from_slice(&[10, 20, 30]);
        }
    }
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "native-rgb");
    let output = decoder_frame(
        &frame.freeze(),
        PixelFormat::Rgba32,
        &copies,
        &HeapOutputAllocator,
    )
    .unwrap();
    assert_eq!(output.len(), 60);
    assert!(output
        .chunks_exact(4)
        .all(|pixel| pixel == [10, 20, 30, 255]));
}

struct NeverAllocate;
impl OutputAllocator for NeverAllocate {
    fn allocate_bytes(
        &self,
        _: usize,
    ) -> std::result::Result<BufferLoan<u8>, BufferAllocationError> {
        panic!("allocation must not occur")
    }
    fn allocate_audio(
        &self,
        _: usize,
    ) -> std::result::Result<BufferLoan<f32>, BufferAllocationError> {
        panic!("audio allocation must not occur")
    }
}

#[test]
fn codec_decoded_strict_rejection_precedes_conversion_and_allocation() {
    let (frame, _) = native_frame();
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-output");
    assert!(matches!(
        decoder_frame(&frame, PixelFormat::Rgba32, &copies, &NeverAllocate),
        Err(CodecError::CopyRejected(_))
    ));
    assert_eq!(
        (
            counts(&copies).rejected_bytes,
            counts(&copies).completed,
            counts(&copies).failed
        ),
        (2448, 0, 0)
    );
}

fn encoded_frame() -> RuntimeData {
    let (_, bytes) = native_frame();
    FFmpegEncoder::new(VideoEncoderConfig {
        codec: VideoCodec::H264,
        hardware_accel: false,
        ..Default::default()
    })
    .unwrap()
    .encode(RuntimeData::Video {
        pixel_data: bytes.into(),
        width: 34,
        height: 18,
        format: PixelFormat::Yuv420p,
        codec: None,
        frame_number: 9,
        timestamp_us: 456,
        is_keyframe: false,
        stream_id: None,
        arrival_ts_us: None,
    })
    .unwrap()
}

async fn invoke(
    node: &VideoDecoderNode,
    input: RuntimeData,
    ctx: &NodeRuntimeContext,
    hook: usize,
) -> crate::Result<RuntimeData> {
    match hook {
        0 => node.process_with_ctx(input, ctx).await,
        1 => {
            node.process_multi_with_ctx(
                std::collections::HashMap::from([("in".into(), input)]),
                ctx,
            )
            .await
        }
        _ => {
            let mut output = None;
            node.process_streaming_with_ctx(input, ctx, |value| {
                output = Some(value);
                Ok(())
            })
            .await?;
            Ok(output.expect("successful decode must invoke callback"))
        }
    }
}

#[tokio::test]
async fn codec_decoded_lenient_hooks_never_swallow_copy_rejection() {
    let packet = encoded_frame();
    let mut ctx = NodeRuntimeContext::for_test("decode-test", "decoder");
    ctx.materialization = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-decoder");
    for hook in 0..3 {
        let node = VideoDecoderNode::new(VideoDecoderConfig::default()).unwrap();
        assert!(invoke(&node, packet.clone(), &ctx, hook)
            .await
            .unwrap_err()
            .to_string()
            .contains("zero-copy"));
    }
    assert_eq!(
        (
            counts(&ctx.materialization).rejected_bytes,
            counts(&ctx.materialization).copied_bytes
        ),
        (3 * 918, 0)
    );
}

#[cfg(feature = "native-worker")]
#[path = "decoded_frames_mapped_tests.rs"]
mod mapped;
