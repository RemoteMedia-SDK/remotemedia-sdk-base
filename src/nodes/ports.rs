//! Re-exports of snapshot port primitives from `remotemedia-traits`.
//!
//! Snapshot ports are an atomic publish/load slot used between a producer
//! (Reactive node) and a consumer (Clocked node) when the consumer wants
//! "the latest published value" rather than "every value the producer
//! ever emitted." Reads never block; writes never block; the consumer
//! always observes the most recent fully-written value.
//!
//! Spec: see `pipeline-pacing` and `node-runtime-context` capabilities.
//!
//! These types moved out of core in Task A4 — see
//! `remotemedia_traits::ports`. This module preserves the historical
//! `crate::nodes::ports::*` paths.

pub use remotemedia_traits::ports::{
    InputPort, OutputPort, PortKind, SnapshotPort, TimestampedSnapshot,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Latest publish wins on a single-thread round-trip.
    #[test]
    fn publish_then_snapshot_returns_latest() {
        let port: OutputPort<u32> = OutputPort::empty();
        assert!(port.snapshot().is_none());

        port.publish(1, 1000);
        port.publish(2, 2000);

        let snap = port.snapshot().expect("populated");
        assert_eq!(snap.value, 2);
        assert_eq!(snap.pts_us, 2000);
        // seq is 0-indexed; second publish gets seq=1.
        assert_eq!(snap.seq, 1);
    }

    /// `InputPort::snapshot` reads from the same slot. Multiple
    /// consumers see the producer's latest publish.
    #[test]
    fn input_port_reads_latest_publish() {
        let out: OutputPort<String> = OutputPort::empty();
        let a = out.input();
        let b = out.input();

        out.publish("hello".to_string(), 100);
        assert_eq!(a.snapshot().unwrap().value, "hello");
        assert_eq!(b.snapshot().unwrap().value, "hello");

        out.publish("world".to_string(), 200);
        assert_eq!(a.snapshot().unwrap().value, "world");
        assert_eq!(b.snapshot().unwrap().value, "world");
    }

    /// Concurrent producer + consumer never observes a torn value.
    #[test]
    fn concurrent_publish_and_snapshot_is_atomic() {
        let port: Arc<OutputPort<Vec<u8>>> = Arc::new(OutputPort::empty());
        port.publish(vec![0; 1024], 0);

        let producer_port = Arc::clone(&port);
        let producer = std::thread::spawn(move || {
            for i in 1..=1000u32 {
                let v = vec![(i & 0xff) as u8; 1024];
                producer_port.publish(v, i as u64 * 1000);
            }
        });

        let consumer_port = Arc::clone(&port);
        let consumer = std::thread::spawn(move || {
            for _ in 0..1000 {
                if let Some(snap) = consumer_port.snapshot() {
                    let expected = snap.value[0];
                    assert!(snap.value.iter().all(|&b| b == expected));
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    /// `SnapshotPort` is dyn-compatible and downcasts back to the
    /// concrete `InputPort<T>`.
    #[test]
    fn snapshot_port_downcasts_to_typed_input() {
        let out: OutputPort<u32> = OutputPort::empty();
        out.publish(42, 100);

        let typed = out.input();
        let erased: Arc<dyn SnapshotPort> = Arc::new(typed);

        let recovered = erased
            .as_any()
            .downcast_ref::<InputPort<u32>>()
            .expect("downcast to InputPort<u32>");
        let snap = recovered.snapshot().expect("populated");
        assert_eq!(snap.value, 42);
        assert_eq!(snap.pts_us, 100);
    }

    /// Wrong-type downcast returns `None` rather than panicking.
    #[test]
    fn wrong_type_downcast_returns_none() {
        let out: OutputPort<u32> = OutputPort::empty();
        out.publish(1, 0);
        let erased: Arc<dyn SnapshotPort> = Arc::new(out.input());
        assert!(erased
            .as_any()
            .downcast_ref::<InputPort<String>>()
            .is_none());
    }
}
