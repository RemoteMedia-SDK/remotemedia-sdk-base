use super::*;
use remotemedia_types::{BufferLoan, CopyCounts};
use std::sync::atomic::{AtomicUsize, Ordering};

mod fixtures;

struct Fixture {
    registry: InProcessChannelRegistry,
    handle: ChannelHandle,
    publisher: InProcessPublisher,
    subscriber: InProcessSubscriber,
    copies: CopyContext,
}
impl Fixture {
    async fn new(policy: CopyPolicy, capacity: usize, backpressure: bool) -> Self {
        let registry = InProcessChannelRegistry::new_internal();
        let copies = CopyContext::new(policy, "test-channel");
        let handle = registry
            .create_channel_with_copy_context("channel", capacity, backpressure, copies.clone())
            .await
            .unwrap();
        let publisher = registry.create_publisher("channel").await.unwrap();
        let subscriber = registry.create_subscriber("channel").await.unwrap();
        Self {
            registry,
            handle,
            publisher,
            subscriber,
            copies,
        }
    }
}

fn counts(copies: &CopyContext, reason: CopyReason) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[tokio::test]
async fn strict_metadata_and_empty_frames_preserve_message_boundaries() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 2, true).await;
    for data in [
        RuntimeData::Text("metadata".into()),
        RuntimeData::Binary(Vec::new().into()),
    ] {
        f.publisher.publish(&data).unwrap();
        assert_eq!(f.subscriber.receive().unwrap().unwrap(), data);
    }
    f.publisher.send(&[]).unwrap();
    assert!(f.subscriber.receive_bytes().unwrap().unwrap().is_empty());
    f.publisher.send_owned(Vec::new().into()).unwrap();
    assert!(f
        .subscriber
        .receive_owned_bytes()
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(f.subscriber.receive_owned_bytes().unwrap().is_none());
    assert!(f
        .copies
        .report()
        .entries
        .iter()
        .all(|(_, count)| count.copied_bytes == 0 && count.rejected == 0));
}

#[tokio::test]
async fn bounded_history_is_available_to_late_subscribers() {
    let registry = InProcessChannelRegistry::new_internal();
    let handle = registry.create_channel("late", 2, true).await.unwrap();
    let publisher = registry.create_publisher("late").await.unwrap();
    publisher.send_owned(vec![1].into()).unwrap();
    publisher.send_owned(vec![2].into()).unwrap();
    assert!(publisher.send_owned(vec![3].into()).is_err());
    let mut subscriber = registry.create_subscriber("late").await.unwrap();
    assert_eq!(
        subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[1]
    );
    assert_eq!(
        subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[2]
    );
    assert!(subscriber.receive_owned_bytes().unwrap().is_none());
    registry.destroy_channel(handle).await.unwrap();
}

#[tokio::test]
async fn typed_frames_are_independent_and_account_all_bulk_variants() {
    let mut f = Fixture::new(CopyPolicy::AllowCopies, 8, true).await;
    assert!(f.copies.same_authority(&f.publisher.copy_context()));
    assert!(f.copies.same_authority(&f.subscriber.copy_context()));
    let data = fixtures::bulk_cases();
    let total: u64 = data
        .iter()
        .map(|data| data.bulk_payload_bytes() as u64)
        .sum();
    for value in &data {
        f.publisher.publish(value).unwrap();
    }
    for value in data {
        assert_eq!(f.subscriber.receive().unwrap().unwrap(), value);
    }
    assert!(f.subscriber.receive().unwrap().is_none());
    assert_eq!(
        counts(&f.copies, CopyReason::PortableSerialization).copied_bytes,
        total
    );
    assert_eq!(
        counts(&f.copies, CopyReason::PortableDeserialization).copied_bytes,
        total
    );
    assert_eq!(
        counts(&f.copies, CopyReason::VecMaterialization).copied_bytes,
        0
    );
    assert_eq!(counts(&f.copies, CopyReason::BorrowedInput).copied_bytes, 0);
}

#[tokio::test]
async fn strict_typed_copies_reject_without_losing_frames() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 1, false).await;
    for data in fixtures::bulk_cases() {
        assert!(f.publisher.publish(&data).is_err());
        assert!(f.subscriber.receive_owned_bytes().unwrap().is_none());
        let frame: SharedBytes = super::super::data_transfer::to_bytes(&data).unwrap().into();
        let pointer = frame.as_slice().as_ptr();
        f.publisher.send_owned(frame).unwrap();
        assert!(f.subscriber.receive().is_err());
        let retained = f.subscriber.receive_owned_bytes().unwrap().unwrap();
        assert_eq!(retained.as_slice().as_ptr(), pointer);
    }
    for reason in [
        CopyReason::PortableSerialization,
        CopyReason::PortableDeserialization,
    ] {
        let count = counts(&f.copies, reason);
        assert_eq!(count.copied_bytes, 0);
        assert_eq!(count.rejected, 6);
    }
}

#[tokio::test]
async fn borrowed_send_and_legacy_snapshot_have_exact_separate_counts() {
    let mut f = Fixture::new(CopyPolicy::AllowCopies, 4, true).await;
    f.publisher.send(b"ab").unwrap();
    f.publisher.send(b"cde").unwrap();
    assert_eq!(f.subscriber.receive_bytes().unwrap().unwrap(), b"abcde");
    assert!(f.subscriber.receive_bytes().unwrap().is_none());
    assert_eq!(counts(&f.copies, CopyReason::BorrowedInput).copied_bytes, 5);
    assert_eq!(
        counts(&f.copies, CopyReason::VecMaterialization).copied_bytes,
        5
    );
    assert_eq!(
        counts(&f.copies, CopyReason::PortableSerialization).copied_bytes,
        0
    );
}

#[tokio::test]
async fn strict_snapshot_rejection_leaves_owning_receive_available() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 2, true).await;
    assert!(f.publisher.send(b"borrowed").is_err());
    let frame = SharedBytes::from(vec![1, 2, 3]);
    f.publisher.send_owned(frame.clone()).unwrap();
    assert!(f.subscriber.receive_bytes().is_err());
    assert!(f
        .subscriber
        .receive_owned_bytes()
        .unwrap()
        .unwrap()
        .shares_storage(&frame));
    assert_eq!(counts(&f.copies, CopyReason::BorrowedInput).copied_bytes, 0);
    assert_eq!(
        counts(&f.copies, CopyReason::VecMaterialization).copied_bytes,
        0
    );
}

struct Allocation {
    bytes: Vec<u8>,
    dropped: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Allocation {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Allocation {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn producer_loan_slices_fanout_and_last_release_preserve_storage() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 1, true).await;
    let mut second = f.registry.create_subscriber("channel").await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut loan = BufferLoan::from_owner(Allocation {
        bytes: vec![0; 16],
        dropped: dropped.clone(),
    });
    loan.as_mut_slice().copy_from_slice(b"0123456789abcdef");
    let pointer = loan.as_mut_slice()[4..].as_ptr();
    let frame = loan.freeze().slice(4..12).unwrap();
    f.publisher.send_owned(frame).unwrap();
    let a = f.subscriber.receive_owned_bytes().unwrap().unwrap();
    let b = second.receive_owned_bytes().unwrap().unwrap();
    assert_eq!(a.as_slice().as_ptr(), pointer);
    assert!(a.shares_storage(&b));
    f.registry.destroy_channel(f.handle).await.unwrap();
    drop(f.publisher);
    drop(f.subscriber);
    drop(second);
    assert_eq!(b.as_slice(), b"456789ab");
    drop(a);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(b);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(f
        .copies
        .report()
        .entries
        .iter()
        .all(|(_, count)| count.copied_bytes == 0));
}

#[tokio::test]
async fn slow_reader_holds_capacity_and_drop_recovers_it_before_encoding() {
    let mut f = Fixture::new(CopyPolicy::AllowCopies, 1, true).await;
    let slow = f.registry.create_subscriber("channel").await.unwrap();
    f.publisher.send_owned(vec![1].into()).unwrap();
    let retained = f.subscriber.receive_owned_bytes().unwrap().unwrap();
    let data = RuntimeData::Binary(vec![2; 8].into());
    assert!(!f.publisher.try_publish(&data).unwrap());
    assert_eq!(
        counts(&f.copies, CopyReason::PortableSerialization).copied_bytes,
        0
    );
    drop(slow);
    assert!(f.publisher.try_publish(&data).unwrap());
    assert_eq!(f.subscriber.receive().unwrap().unwrap(), data);
    assert_eq!(retained.as_slice(), &[1]);
    assert_eq!(
        counts(&f.copies, CopyReason::PortableSerialization).copied_bytes,
        8
    );
}

#[tokio::test]
async fn overwrite_and_drain_report_lag_then_resume_at_valid_frame_boundaries() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 2, false).await;
    for value in [1, 2, 3] {
        f.publisher.send_owned(vec![value].into()).unwrap();
    }
    assert!(f
        .subscriber
        .receive_owned_bytes()
        .unwrap_err()
        .to_string()
        .contains("missed 1 frames"));
    assert_eq!(
        f.subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[2]
    );
    assert_eq!(f.registry.drain_channel("channel").await.unwrap(), 1);
    f.publisher.send_owned(vec![4].into()).unwrap();
    assert!(f.subscriber.receive_owned_bytes().is_err());
    assert_eq!(
        f.subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[4]
    );
    assert!(f.subscriber.receive_owned_bytes().unwrap().is_none());
}

#[tokio::test]
async fn failed_conversion_does_not_evict_a_full_nonblocking_queue() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 1, false).await;
    f.publisher.send_owned(vec![9].into()).unwrap();
    assert!(f
        .publisher
        .publish(&RuntimeData::Binary(vec![1; 8].into()))
        .is_err());
    assert_eq!(
        f.subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[9]
    );
}

#[tokio::test]
async fn malformed_trailing_and_legacy_frames_have_explicit_framing() {
    let mut f = Fixture::new(CopyPolicy::AllowCopies, 4, true).await;
    let text = RuntimeData::Text("metadata".into());
    let valid = super::super::data_transfer::to_bytes(&text).unwrap();
    let mut trailing = valid.clone();
    trailing.extend_from_slice(&valid);
    for frame in [vec![0xc1], valid[..valid.len() - 1].to_vec(), trailing] {
        f.publisher.send_owned(frame.clone().into()).unwrap();
        assert!(f.subscriber.receive().is_err());
        assert_eq!(
            f.subscriber
                .receive_owned_bytes()
                .unwrap()
                .unwrap()
                .as_slice(),
            frame
        );
    }
    let mut legacy = b"RM_AUD_RMP1\0".to_vec();
    legacy.extend(valid);
    f.publisher.send_owned(legacy.into()).unwrap();
    assert_eq!(f.subscriber.receive().unwrap().unwrap(), text);
}

#[tokio::test]
async fn fresh_readers_skip_history_without_changing_existing_readers() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 3, true).await;
    f.publisher.send_owned(vec![1].into()).unwrap();
    let mut fresh = f.registry.create_subscriber_fresh("channel").await.unwrap();
    assert!(fresh.receive_owned_bytes().unwrap().is_none());
    f.publisher.send_owned(vec![2].into()).unwrap();
    assert_eq!(
        fresh.receive_owned_bytes().unwrap().unwrap().as_slice(),
        &[2]
    );
    assert_eq!(
        f.subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[1]
    );
    assert_eq!(
        f.subscriber
            .receive_owned_bytes()
            .unwrap()
            .unwrap()
            .as_slice(),
        &[2]
    );
}

#[tokio::test]
async fn channel_replacement_cannot_rebind_authority_or_revive_old_handles() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 2, true).await;
    assert!(f
        .registry
        .create_channel("channel", 2, false)
        .await
        .is_err());
    f.registry.destroy_channel(f.handle.clone()).await.unwrap();
    assert!(f.publisher.send_owned(vec![1].into()).is_err());
    assert!(f.subscriber.receive_owned_bytes().unwrap().is_none());
    let replacement = f.registry.create_channel("channel", 2, true).await.unwrap();
    assert!(f.registry.destroy_channel(f.handle).await.is_err());
    let publisher = f.registry.create_publisher("channel").await.unwrap();
    assert!(!publisher.copy_context().same_authority(&f.copies));
    assert_eq!(publisher.copy_context().policy(), CopyPolicy::AllowCopies);
    f.registry.destroy_channel(replacement).await.unwrap();
    assert!(f.registry.create_channel("zero", 0, false).await.is_err());
}

#[tokio::test]
async fn concurrent_publishers_preserve_each_frame_and_per_producer_order() {
    let mut f = Fixture::new(CopyPolicy::RequireZeroCopy, 128, true).await;
    let mut second = f.registry.create_subscriber("channel").await.unwrap();
    let publisher = Arc::new(f.publisher);
    let tasks: Vec<_> = (0..4u8)
        .map(|producer| {
            let publisher = publisher.clone();
            std::thread::spawn(move || {
                for sequence in 0..32u8 {
                    publisher
                        .send_owned(vec![producer, sequence].into())
                        .unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.join().unwrap();
    }
    let mut expected = [0u8; 4];
    for _ in 0..128 {
        let first = f.subscriber.receive_owned_bytes().unwrap().unwrap();
        let other = second.receive_owned_bytes().unwrap().unwrap();
        assert!(first.shares_storage(&other));
        let bytes = first.as_slice();
        assert_eq!(bytes[1], expected[bytes[0] as usize]);
        expected[bytes[0] as usize] += 1;
    }
    assert_eq!(expected, [32; 4]);
    assert!(f
        .copies
        .report()
        .entries
        .iter()
        .all(|(_, count)| count.copied_bytes == 0));
}
