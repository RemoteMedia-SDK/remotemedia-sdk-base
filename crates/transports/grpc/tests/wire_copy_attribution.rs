//! Task 6.6: the gRPC wire adapters' copies, observed independently of their
//! ledgers. A counting global allocator records host heap allocations inside
//! a probe window:
//! - audio, which needs f32 <-> little-endian conversion, makes exactly the
//!   counted copy in each direction;
//! - byte-backed payloads move into shared owners on decode with no copy and
//!   no count;
//! - strict policy rejects before any payload-sized allocation.
use remotemedia_core::data::{CopyContext, CopyPolicy, CopyReason, RuntimeData};
use remotemedia_grpc::adapters::{
    data_buffer_into_runtime_data_with_context, runtime_data_to_data_buffer_with_context,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex,
};

struct Counting;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LARGEST: AtomicU64 = AtomicU64::new(0);
static SERIAL: Mutex<()> = Mutex::new(());

fn record(size: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
        LARGEST.fetch_max(size as u64, Ordering::Relaxed);
    }
}
// SAFETY: forwards to the system allocator; counters are relaxed atomics.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// (total heap bytes, largest single allocation) during `work`.
fn probe<T>(work: impl FnOnce() -> T) -> (T, u64, u64) {
    BYTES.store(0, Ordering::Relaxed);
    LARGEST.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::SeqCst);
    let value = work();
    ACTIVE.store(false, Ordering::SeqCst);
    (value, BYTES.load(Ordering::Relaxed), LARGEST.load(Ordering::Relaxed))
}

fn counted(copies: &CopyContext, reason: CopyReason) -> u64 {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .map_or(0, |(_, c)| c.copied_bytes)
}

const PAYLOAD: usize = 38_400;

#[test]
fn audio_conversion_copies_match_their_counts_in_both_directions() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let audio = RuntimeData::Audio {
        samples: (0..PAYLOAD / 4).map(|i| i as f32 * 0.5).collect::<Vec<f32>>().into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    let encoding = CopyContext::new(CopyPolicy::AllowCopies, "grpc-encode");
    let (buffer, encode_bytes, encode_largest) =
        probe(|| runtime_data_to_data_buffer_with_context(&audio, &encoding).unwrap());
    let decoding = CopyContext::new(CopyPolicy::AllowCopies, "grpc-decode");
    let (decoded, decode_bytes, decode_largest) =
        probe(|| data_buffer_into_runtime_data_with_context(buffer, None, &decoding).unwrap());
    let decoded = decoded.unwrap();
    assert_eq!(decoded.bulk_payload_bytes(), PAYLOAD);
    println!(
        "GRPC_WIRE_PROBE {}",
        serde_json::json!({"payload": PAYLOAD,
            "encode": {"counted": counted(&encoding, CopyReason::PortableSerialization),
                       "heap_bytes": encode_bytes, "largest": encode_largest},
            "decode": {"counted": counted(&decoding, CopyReason::PortableDeserialization),
                       "heap_bytes": decode_bytes, "largest": decode_largest}})
    );
    assert_eq!(counted(&encoding, CopyReason::PortableSerialization), PAYLOAD as u64);
    assert_eq!(counted(&decoding, CopyReason::PortableDeserialization), PAYLOAD as u64);
    for (side, bytes, largest) in [
        ("encode", encode_bytes, encode_largest),
        ("decode", decode_bytes, decode_largest),
    ] {
        assert!(largest >= PAYLOAD as u64, "{side}: no payload-sized destination");
        assert!(bytes <= 4 * PAYLOAD as u64 + 16 * 1024, "{side}: more than one counted copy");
    }
}

#[test]
fn byte_payloads_move_into_shared_owners_without_copying() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let binary = RuntimeData::Binary(vec![7u8; PAYLOAD].into());
    let encoding = CopyContext::new(CopyPolicy::AllowCopies, "grpc-encode");
    let buffer = runtime_data_to_data_buffer_with_context(&binary, &encoding).unwrap();
    // The owned protobuf message's bytes move into the runtime owner.
    let decoding = CopyContext::new(CopyPolicy::RequireZeroCopy, "grpc-decode-strict");
    let (decoded, bytes, largest) =
        probe(|| data_buffer_into_runtime_data_with_context(buffer, None, &decoding).unwrap());
    let decoded = decoded.unwrap();
    assert_eq!(decoded.bulk_payload_bytes(), PAYLOAD);
    println!(
        "GRPC_WIRE_PROBE {}",
        serde_json::json!({"payload": PAYLOAD, "binary_decode": {
            "counted": counted(&decoding, CopyReason::PortableDeserialization),
            "heap_bytes": bytes, "largest": largest}})
    );
    assert_eq!(counted(&decoding, CopyReason::PortableDeserialization), 0);
    assert!(largest < PAYLOAD as u64, "binary decode copied its payload ({largest} B)");
}

#[test]
fn strict_encode_rejects_before_any_payload_copy() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let binary = RuntimeData::Binary(vec![1u8; PAYLOAD].into());
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "grpc-strict");
    let (result, _, largest) = probe(|| runtime_data_to_data_buffer_with_context(&binary, &strict));
    assert!(result.is_err());
    assert!(largest < PAYLOAD as u64, "strict rejection allocated {largest} B");
}
