use crate::{codec, stream};
use bytes::Bytes;
use prost::Message;
use remotemedia_core::{data::RuntimeData, transport::TransportData};
use remotemedia_types::{CopyContext, CopyCounts, CopyPolicy, CopyReason};

fn copies(policy: CopyPolicy) -> CopyContext {
    CopyContext::new(policy, "http-test")
}
fn count(copies: &CopyContext, reason: CopyReason) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}
fn audio() -> TransportData {
    TransportData::new(RuntimeData::Audio {
        samples: vec![0.25, -0.5, 1.0, 0.0].into(),
        sample_rate: 16000,
        channels: 1,
        timestamp_us: Some(123),
        arrival_ts_us: None,
        stream_id: Some("audio".into()),
        metadata: None,
    })
}

pub(crate) fn bulk_cases() -> [RuntimeData; 6] {
    use remotemedia_types::{ImageFormat, PixelFormat};
    [
        audio().data,
        RuntimeData::Binary(vec![1u8, 2, 3].into()),
        RuntimeData::Video {
            pixel_data: vec![0u8; 6].into(),
            width: 2,
            height: 1,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 3,
            is_keyframe: true,
            timestamp_us: 100,
            stream_id: None,
            arrival_ts_us: None,
        },
        RuntimeData::Image {
            data: vec![0u8; 6].into(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            },
            width: 2,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        },
        RuntimeData::Tensor {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: 1,
            metadata: None,
        },
        RuntimeData::Numpy {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: "float32".into(),
            strides: vec![8, 4],
            c_contiguous: true,
            f_contiguous: false,
        },
    ]
}

/// Bytes a decode must copy: only audio samples (to aligned f32s). Other
/// payloads keep the received buffer.
pub(crate) fn decode_copy_bytes(data: &RuntimeData) -> u64 {
    match data {
        RuntimeData::Audio { .. } => data.bulk_payload_bytes() as u64,
        _ => 0,
    }
}

/// One delimited event carrying `frame`.
fn frame_event(frame: Vec<u8>) -> Vec<u8> {
    let mut event = codec::frame_event_header(frame.len()).to_vec();
    event.extend_from_slice(&frame);
    event
}

#[test]
fn protobuf_roundtrip_is_lossless_and_counts_only_real_copies() {
    for data in bulk_cases() {
        let payload = data.bulk_payload_bytes() as u64;
        let mut data = TransportData::new(data);
        data.sequence = Some(9);
        data.metadata.insert("participant.id".into(), "ana".into());
        let ledger = copies(CopyPolicy::AllowCopies);
        let wire = codec::encode_frame(&data, &ledger).unwrap();
        let decoded = codec::decode_frame(Bytes::from(wire.clone()), &ledger).unwrap();
        assert_eq!(decoded.data, data.data);
        assert_eq!(decoded.sequence, Some(9));
        assert_eq!(decoded.metadata, data.metadata);
        assert_eq!(
            count(&ledger, CopyReason::PortableSerialization).copied_bytes,
            payload
        );
        assert_eq!(
            count(&ledger, CopyReason::PortableDeserialization).copied_bytes,
            decode_copy_bytes(&data.data)
        );

        // Strict: encoding any bulk payload is a copy; decoding is one only
        // for audio.
        let strict = copies(CopyPolicy::RequireZeroCopy);
        assert!(codec::encode_frame(&data, &strict).is_err());
        let decoded = codec::decode_frame(Bytes::from(wire), &strict);
        assert_eq!(decoded.is_err(), decode_copy_bytes(&data.data) > 0);
        assert_eq!(
            count(&strict, CopyReason::PortableDeserialization).copied_bytes,
            0
        );
    }
}

#[test]
fn audio_crosses_as_raw_f32_bytes() {
    let wire = codec::encode_frame(&audio(), CopyContext::legacy()).unwrap();
    let raw: Vec<u8> = [0.25f32, -0.5, 1.0, 0.0]
        .iter()
        .flat_map(|sample| sample.to_le_bytes())
        .collect();
    assert!(wire.windows(raw.len()).any(|window| window == raw));
    assert!(!wire.windows(4).any(|window| window == b"0.25"));
}

#[test]
fn malformed_frames_are_invalid_data() {
    let ledger = copies(CopyPolicy::AllowCopies);
    for body in [&b"\xff\xff"[..], &b""[..], &b"{\"data\":1}"[..]] {
        let error = codec::decode_frame(Bytes::copy_from_slice(body), &ledger).unwrap_err();
        assert!(
            matches!(error, remotemedia_core::Error::InvalidData(_))
                || error.to_string().contains("HTTP transport"),
            "{error}"
        );
    }
}

#[test]
fn stream_retains_contiguous_events_and_counts_one_fragment_assembly() {
    let ledger = copies(CopyPolicy::AllowCopies);
    let data = TransportData::new(RuntimeData::Text("héllo 🎧".into()));
    let wire = frame_event(codec::encode_frame(&data, &ledger).unwrap());
    let bytes = Bytes::from(wire.clone());
    let pointer = bytes.as_ptr();
    let mut events = stream::Events::default();
    events.push(bytes);
    let event = events.next(&ledger).unwrap().unwrap();
    assert_eq!(event.as_ptr(), pointer.wrapping_add(1));
    assert_eq!(
        stream::decode(event, &ledger).unwrap().unwrap().data,
        data.data
    );
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    assert!(events.is_empty());
    // Every-byte fragmentation splits the length prefix and the payload.
    for (index, byte) in wire.iter().enumerate() {
        events.push(Bytes::from(vec![*byte]));
        let event = events.next(&ledger).unwrap();
        if index + 1 == wire.len() {
            assert_eq!(
                stream::decode(event.unwrap(), &ledger)
                    .unwrap()
                    .unwrap()
                    .data,
                data.data
            );
        } else {
            assert!(event.is_none());
        }
    }
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        wire.len() as u64 - 1
    );
}

#[test]
fn stream_strict_staging_rejection_preserves_chunks_and_errors_are_visible() {
    let strict = copies(CopyPolicy::RequireZeroCopy);
    let event = frame_event(
        codec::encode_frame(
            &TransportData::new(RuntimeData::Text("x".into())),
            CopyContext::legacy(),
        )
        .unwrap(),
    );
    let mut events = stream::Events::default();
    events.push(Bytes::copy_from_slice(&event[..3]));
    events.push(Bytes::copy_from_slice(&event[3..]));
    assert!(events.next(&strict).is_err());
    assert_eq!(
        count(&strict, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    // The refused event is still there for a context that may copy.
    let allowed = copies(CopyPolicy::AllowCopies);
    assert!(events.next(&allowed).unwrap().is_some());
    assert!(events.is_empty());

    let error =
        stream::decode(codec::error_event("failed output").slice(1..), &allowed).unwrap_err();
    assert!(error.to_string().contains("failed output"), "{error}");
    events.push(Bytes::from_static(codec::KEEPALIVE));
    let keepalive = events.next(&allowed).unwrap().unwrap();
    assert!(stream::decode(keepalive, &allowed).unwrap().is_none());
    assert!(stream::decode(Bytes::from_static(b"\x0a\x05ab"), &allowed).is_err());
}

#[test]
fn stream_multiple_events_retain_ranges() {
    let ledger = copies(CopyPolicy::AllowCopies);
    let data = TransportData::new(RuntimeData::Text("hello".into()));
    let one = frame_event(codec::encode_frame(&data, &ledger).unwrap());
    let bytes = Bytes::from([one.clone(), one.clone()].concat());
    let pointer = bytes.as_ptr();
    let mut events = stream::Events::default();
    events.push(bytes);
    for offset in [1, one.len() + 1] {
        let event = events.next(&ledger).unwrap().unwrap();
        assert_eq!(event.as_ptr(), pointer.wrapping_add(offset));
        assert_eq!(
            stream::decode(event, &ledger).unwrap().unwrap().data,
            data.data
        );
    }
    assert!(events.is_empty());
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        0
    );
}

struct Fixture {
    url: String,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture(stream_body: Vec<u8>, content_type: &'static str) -> Fixture {
    use axum::{
        body::Bytes,
        extract::State,
        routing::{delete, get, post},
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/execute",
            post(
                |State(calls): State<Arc<AtomicUsize>>, body: Bytes| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let request = codec::ExecuteRequest::decode(body).unwrap();
                    serde_json::from_str::<serde_json::Value>(&request.manifest_json).unwrap();
                    (
                        [("content-type", codec::PROTOBUF)],
                        request.input.unwrap().encode_to_vec(),
                    )
                },
            ),
        )
        .route(
            "/stream",
            post(|| async { Json(serde_json::json!({"session_id":"test"})) }),
        )
        .route(
            "/stream/test/output",
            get(move || {
                let body = stream_body.clone();
                async move { ([("content-type", content_type)], body) }
            }),
        )
        .route(
            "/stream/test/input",
            post(
                |State(calls): State<Arc<AtomicUsize>>, body: Bytes| async move {
                    remotemedia_wire::DataBuffer::decode(body).unwrap();
                    calls.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                },
            ),
        )
        .route(
            "/stream/test",
            delete(|| async { axum::http::StatusCode::OK }),
        )
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Fixture { url, calls, task }
}

fn manifest() -> std::sync::Arc<remotemedia_core::manifest::Manifest> {
    std::sync::Arc::new(
        serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{}, "nodes":[], "connections":[]
        }))
        .unwrap(),
    )
}

#[tokio::test]
async fn real_http_client_accounts_unary_and_stream_sends_and_refuses_strict_before_request() {
    use remotemedia_core::transport::PipelineClient;
    use std::sync::atomic::Ordering;
    let fixture = fixture(codec::KEEPALIVE.to_vec(), codec::PROTOBUF_STREAM).await;
    let ledger = copies(CopyPolicy::AllowCopies);
    let client = crate::HttpPipelineClient::new(&fixture.url, None)
        .await
        .unwrap()
        .with_copy_context(ledger.clone());
    assert!(client.copy_context().same_authority(&ledger));
    let output = client
        .execute_unary(manifest(), audio(), &[], None)
        .await
        .unwrap();
    assert_eq!(output.data, audio().data);
    assert_eq!(
        count(&ledger, CopyReason::PortableSerialization).copied_bytes,
        16
    );
    assert_eq!(
        count(&ledger, CopyReason::PortableDeserialization).copied_bytes,
        16
    );
    let mut stream = client
        .create_stream_session(manifest(), &[], None)
        .await
        .unwrap();
    stream.send(audio()).await.unwrap();
    stream.close().await.unwrap();
    stream.close().await.unwrap();
    assert_eq!(
        count(&ledger, CopyReason::PortableSerialization).copied_bytes,
        32
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    let client = client.with_copy_context(copies(CopyPolicy::RequireZeroCopy));
    assert!(client
        .execute_unary(manifest(), audio(), &[], None)
        .await
        .is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn real_stream_client_surfaces_parse_server_truncation_and_type_errors() {
    use remotemedia_core::transport::PipelineClient;
    let server_error = codec::error_event("server sentinel").to_vec();
    for (body, content_type, expected) in [
        (
            vec![2, 0xff, 0xff],
            codec::PROTOBUF_STREAM,
            "HTTP transport",
        ),
        (server_error, codec::PROTOBUF_STREAM, "server sentinel"),
        (vec![9, 0x0a], codec::PROTOBUF_STREAM, "incomplete event"),
        (
            b"data: {}\n\n".to_vec(),
            "text/event-stream",
            "content-type",
        ),
    ] {
        let fixture = fixture(body, content_type).await;
        let client = crate::HttpPipelineClient::new(&fixture.url, None)
            .await
            .unwrap();
        let mut stream = client
            .create_stream_session(manifest(), &[], None)
            .await
            .unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(3), stream.receive())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert!(!stream.is_active());
        stream.close().await.unwrap();
    }
}

#[tokio::test]
async fn real_stream_bulk_output_uses_retained_caller_authority() {
    use remotemedia_core::transport::PipelineClient;
    let event = frame_event(codec::encode_frame(&audio(), CopyContext::legacy()).unwrap());
    let fixture = fixture(event, codec::PROTOBUF_STREAM).await;
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let ledger = copies(policy);
        let client = crate::HttpPipelineClient::new(&fixture.url, None)
            .await
            .unwrap()
            .with_copy_context(ledger.clone());
        let mut stream = client
            .create_stream_session(manifest(), &[], None)
            .await
            .unwrap();
        drop(client);
        let output = tokio::time::timeout(std::time::Duration::from_secs(3), stream.receive())
            .await
            .unwrap();
        if policy == CopyPolicy::AllowCopies {
            assert_eq!(output.unwrap().unwrap().data, audio().data);
            assert_eq!(
                count(&ledger, CopyReason::PortableDeserialization).copied_bytes,
                16
            );
        } else {
            assert!(output.is_err());
            assert_eq!(
                count(&ledger, CopyReason::PortableDeserialization).copied_bytes,
                0
            );
        }
        stream.close().await.unwrap();
        assert!(ledger
            .report()
            .entries
            .iter()
            .all(|(_, count)| count.in_flight == 0));
    }
}
