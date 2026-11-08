use crate::{codec, sse};
use bytes::Bytes;
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

#[test]
fn json_roundtrip_attributes_all_bulk_types_and_rejects_strict_conversion() {
    for data in bulk_cases() {
        let payload = data.bulk_payload_bytes();
        let data = TransportData::new(data);
        let ledger = copies(CopyPolicy::AllowCopies);
        let wire = codec::encode(&data, payload, &ledger).unwrap();
        let decoded: TransportData = codec::decode(&wire, &ledger).unwrap();
        assert_eq!(decoded.data, data.data);
        assert_eq!(
            count(&ledger, CopyReason::PortableSerialization).copied_bytes,
            payload as u64
        );
        assert_eq!(
            count(&ledger, CopyReason::PortableDeserialization).copied_bytes,
            payload as u64
        );
        let strict = copies(CopyPolicy::RequireZeroCopy);
        assert!(codec::encode(&data, payload, &strict).is_err());
        assert!(codec::decode::<TransportData>(&wire, &strict).is_err());
        assert_eq!(
            count(&strict, CopyReason::PortableDeserialization).copied_bytes,
            0
        );
    }
}

#[test]
fn json_failures_are_not_reported_as_completed_encoding() {
    struct Broken;
    impl serde::Serialize for Broken {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("encoding sentinel"))
        }
    }
    let ledger = copies(CopyPolicy::AllowCopies);
    assert!(codec::encode(&Broken, 12, &ledger)
        .unwrap_err()
        .to_string()
        .contains("sentinel"));
    let result = count(&ledger, CopyReason::PortableSerialization);
    assert_eq!(result.copied_bytes, 0);
    assert_eq!(result.failed_requested_bytes, 12);
    assert_eq!(result.in_flight, 0);
    let strict = copies(CopyPolicy::RequireZeroCopy);
    assert!(!codec::encode(&Broken, 12, &strict)
        .unwrap_err()
        .to_string()
        .contains("sentinel"));
    let valid = serde_json::to_string(&audio()).unwrap();
    assert!(
        codec::decode::<TransportData>(format!("{valid} trailing").as_bytes(), &ledger).is_err()
    );
}

#[test]
fn sse_retains_contiguous_wire_and_counts_one_fragment_assembly() {
    let ledger = copies(CopyPolicy::AllowCopies);
    let data = TransportData::new(RuntimeData::Text("héllo 🎧".into()));
    let wire = format!("data: {}\r\n\r\n", serde_json::to_string(&data).unwrap());
    let bytes = Bytes::from(wire.clone());
    let pointer = bytes.as_ptr();
    let mut frames = sse::Frames::default();
    frames.push(bytes);
    let frame = frames.next(&ledger).unwrap().unwrap();
    assert_eq!(frame.as_ptr(), pointer);
    assert_eq!(
        sse::decode(&frame, &ledger).unwrap().unwrap().data,
        data.data
    );
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    assert!(frames.is_empty());
    // Every-byte fragmentation exercises split UTF-8 and CRLF boundaries.
    for (index, byte) in wire.bytes().enumerate() {
        frames.push(Bytes::from(vec![byte]));
        let frame = frames.next(&ledger).unwrap();
        if index + 1 == wire.len() {
            assert_eq!(
                sse::decode(&frame.unwrap(), &ledger).unwrap().unwrap().data,
                data.data
            );
        } else {
            assert!(frame.is_none());
        }
    }
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        wire.len() as u64
    );
}

#[test]
fn sse_strict_staging_rejection_preserves_chunks_and_errors_are_visible() {
    let strict = copies(CopyPolicy::RequireZeroCopy);
    let mut frames = sse::Frames::default();
    frames.push(Bytes::from_static(b"data: "));
    frames.push(Bytes::from_static(b"{}\n\n"));
    assert!(frames.next(&strict).is_err());
    assert_eq!(
        count(&strict, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    let allowed = copies(CopyPolicy::AllowCopies);
    assert!(sse::decode(b"data: broken\n\n", &allowed).is_err());
    assert!(
        sse::decode(b"event: error\ndata: failed output\n\n", &allowed)
            .unwrap_err()
            .to_string()
            .contains("failed output")
    );
    assert!(sse::decode(b": keepalive\n\n", &allowed).unwrap().is_none());
}

#[test]
fn sse_multiple_events_retain_ranges_and_multiline_json_counts_assembly() {
    let ledger = copies(CopyPolicy::AllowCopies);
    let data = TransportData::new(RuntimeData::Text("hello".into()));
    let json = serde_json::to_string(&data).unwrap();
    let first = format!("data: {json}\n\n");
    let bytes = Bytes::from(format!("{first}{first}"));
    let pointer = bytes.as_ptr();
    let mut frames = sse::Frames::default();
    frames.push(bytes);
    for offset in [0, first.len()] {
        let frame = frames.next(&ledger).unwrap().unwrap();
        assert_eq!(frame.as_ptr(), pointer.wrapping_add(offset));
        assert_eq!(
            sse::decode(&frame, &ledger).unwrap().unwrap().data,
            data.data
        );
    }
    assert!(frames.is_empty());
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    let multiline = format!("data: {{\ndata: {}\n\n", &json[1..]);
    assert_eq!(
        sse::decode(multiline.as_bytes(), &ledger)
            .unwrap()
            .unwrap()
            .data,
        data.data
    );
    assert_eq!(
        count(&ledger, CopyReason::VecMaterialization).copied_bytes,
        json.len() as u64 + 1
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

async fn fixture(sse_body: impl Into<String>) -> Fixture {
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
    let sse_body = sse_body.into();
    let router = Router::new()
        .route(
            "/execute",
            post(
                |State(calls): State<Arc<AtomicUsize>>, body: Bytes| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    Json(serde_json::json!({"output": value["input"]}))
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
                let body = sse_body.clone();
                async move { ([("content-type", "text/event-stream")], body) }
            }),
        )
        .route(
            "/stream/test/input",
            post(
                |State(calls): State<Arc<AtomicUsize>>, body: Bytes| async move {
                    let _: serde_json::Value = serde_json::from_slice(&body).unwrap();
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
    let fixture = fixture(": keepalive\n\n").await;
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
async fn real_sse_client_surfaces_parse_server_and_truncated_event_errors() {
    use remotemedia_core::transport::PipelineClient;
    for (body, expected) in [
        ("data: broken\n\n", "HTTP compatibility"),
        ("event: error\ndata: server sentinel\n\n", "server sentinel"),
        ("data: incomplete", "incomplete event"),
    ] {
        let fixture = fixture(body).await;
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
async fn real_sse_bulk_output_uses_retained_caller_authority() {
    use remotemedia_core::transport::PipelineClient;
    let fixture = fixture(format!(
        "data: {}\n\n",
        serde_json::to_string(&audio()).unwrap()
    ))
    .await;
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
