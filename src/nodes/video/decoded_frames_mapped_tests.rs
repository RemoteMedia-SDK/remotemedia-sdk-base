use super::*;
use crate::multiprocess::native_worker::{PoolConfig, SessionId, SharedPool, SizeClass};

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId(*uuid::Uuid::new_v4().as_bytes()),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 8,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn codec_decoded_retained_view_outlives_decoder_and_context() {
    let pool = pool();
    let packet = encoded_frame();
    let node = VideoDecoderNode::new(VideoDecoderConfig::default()).unwrap();
    let ctx = NodeRuntimeContext::for_test("retained", "decoder")
        .with_output_allocator(Arc::new(pool.clone()));
    let output = node.process_with_ctx(packet, &ctx).await.unwrap();
    let RuntimeData::Video { pixel_data, .. } = output else {
        panic!("expected video")
    };
    let retained = pixel_data.slice(610..620).unwrap();
    let expected = retained.as_slice().to_vec(); // Fixture snapshot, not pipeline staging.
    drop((pixel_data, node, ctx));
    assert!(
        pool.allocate_bytes(1).is_err(),
        "retained view must keep its slot"
    );
    std::thread::spawn(move || assert_eq!(retained.as_slice(), expected))
        .join()
        .unwrap();
    assert!(
        pool.allocate_bytes(4096).is_ok(),
        "last-view release must reclaim storage"
    );
}

#[test]
fn codec_decoded_mapped_output_survives_frame_and_recovers_after_exhaustion() {
    let pool = pool();
    let (frame, expected) = native_frame();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "mapped-decoded");
    let output = decoder_frame(&frame, PixelFormat::Yuv420p, &copies, &pool).unwrap();
    assert!(pool.owns_buffer(&output));
    assert_eq!(output.as_slice(), expected);
    let retained = output.slice(610..620).unwrap();
    drop(output);
    assert!(matches!(
        decoder_frame(&frame, PixelFormat::Yuv420p, &copies, &pool),
        Err(CodecError::OutputAllocation(_))
    ));
    assert_eq!(
        (
            counts(&copies).completed,
            counts(&copies).failed,
            counts(&copies).copied_bytes
        ),
        (1, 1, 918)
    );
    drop(frame);
    std::thread::spawn(move || assert_eq!(retained.as_slice(), &expected[610..620]))
        .join()
        .unwrap();
    let (frame, expected) = native_frame();
    let recovered = decoder_frame(&frame, PixelFormat::I420, &copies, &pool).unwrap();
    assert_eq!(recovered.as_slice(), expected);
    assert_eq!((counts(&copies).completed, counts(&copies).failed), (2, 1));
}

#[tokio::test]
async fn codec_decoded_real_hooks_use_selected_pool_and_keep_layout_errors_fatal() {
    let pool = pool();
    let packet = encoded_frame();
    for format in [
        PixelFormat::Yuv420p,
        PixelFormat::I420,
        PixelFormat::NV12,
        PixelFormat::Rgb24,
        PixelFormat::Rgba32,
    ] {
        let node = VideoDecoderNode::new(VideoDecoderConfig {
            output_format: format,
            ..Default::default()
        })
        .unwrap();
        let mut ctx = NodeRuntimeContext::for_test("decode-test", "decoder")
            .with_output_allocator(Arc::new(pool.clone()));
        ctx.materialization = CopyContext::new(CopyPolicy::AllowCopies, "mapped-decoder");
        let bytes = Layout::new(34, 18, format).unwrap().bytes;
        for hook in 0..3 {
            let output = invoke(&node, packet.clone(), &ctx, hook).await.unwrap();
            assert!(pool.owns_payload(&output));
            let RuntimeData::Video {
                pixel_data,
                width,
                height,
                format: actual,
                frame_number,
                timestamp_us,
                ..
            } = output
            else {
                panic!("expected video")
            };
            assert_eq!(
                (width, height, actual, frame_number, timestamp_us),
                (34, 18, format, 9, 456)
            );
            assert_eq!(pixel_data.len(), bytes);
            assert!(invoke(&node, packet.clone(), &ctx, hook)
                .await
                .unwrap_err()
                .to_string()
                .contains("output allocat"));
            drop(pixel_data);
        }
        assert_eq!(
            (
                counts(&ctx.materialization).completed,
                counts(&ctx.materialization).failed,
                counts(&ctx.materialization).copied_bytes
            ),
            (3, 3, 3 * bytes as u64)
        );
        drop(node);
        let output = pool.allocate_bytes(1).unwrap();
        drop(output);
    }
    let invalid = VideoDecoderNode::new(VideoDecoderConfig {
        output_format: PixelFormat::Encoded,
        ..Default::default()
    })
    .unwrap();
    let ctx = NodeRuntimeContext::for_test("decode-test", "invalid");
    assert!(invalid
        .process_with_ctx(packet, &ctx)
        .await
        .unwrap_err()
        .to_string()
        .contains("supported raw pixel format"));
}
