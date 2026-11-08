use super::*;
use iceoryx2::prelude::*;
use napi::bindgen_prelude::Buffer;
use remotemedia_types::{CopyPolicy, CopyReason};
use std::sync::atomic::AtomicUsize;

fn setup(policy: CopyPolicy, limit: usize) -> (NapiPublisher, CopyContext) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let name = format!(
        "napi_loan_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let copies = CopyContext::new(policy, &name);
    let publisher = NapiPublisher::new(
        name,
        1024,
        limit,
        Arc::new(AtomicBool::new(true)),
        copies.clone(),
    )
    .unwrap();
    (publisher, copies)
}

#[test]
fn native_delivery_preserves_retained_sample_and_returns_credits() {
    let (mut publisher, copies) = setup(CopyPolicy::RequireZeroCopy, 1);
    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service = node
        .service_builder(&ServiceName::new(&publisher.channel_name).unwrap())
        .publish_subscribe::<[u8]>()
        .max_publishers(10)
        .max_subscribers(10)
        .history_size(64)
        .subscriber_max_buffer_size(64)
        .open_or_create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();
    let loan = publisher.loan(8).unwrap();
    assert_eq!(publisher.loaned_count(), 1);
    assert!(publisher.try_loan(1).unwrap().is_none());
    loan.fill(7).unwrap();
    loan.set(3, 99).unwrap();
    assert!(loan.set(8, 1).is_err());
    assert!(loan.fill(256).is_err());
    loan.send().unwrap();
    assert_eq!(publisher.loaned_count(), 0);
    assert!(loan.send().is_err());
    assert!(loan.release().is_err());
    assert!(loan.set(0, 1).is_err());
    let received = subscriber
        .receive()
        .unwrap()
        .expect("actual native publication");
    assert_eq!(received.payload(), &[7, 7, 7, 99, 7, 7, 7, 7]);
    let address = received.payload().as_ptr();
    let next = publisher.loan(8).unwrap();
    next.fill(42).unwrap();
    next.send().unwrap();
    publisher.close();
    assert_eq!(received.payload().as_ptr(), address);
    assert_eq!(received.payload(), &[7, 7, 7, 99, 7, 7, 7, 7]);
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}

#[test]
fn release_drop_close_and_failed_allocation_return_credits() {
    let (mut publisher, _) = setup(CopyPolicy::AllowCopies, 1);
    assert!(publisher.loan(1025).is_err());
    assert_eq!(publisher.loaned_count(), 0);
    let loan = publisher.loan(0).unwrap();
    loan.release().unwrap();
    assert!(loan.release().is_err());
    assert_eq!(publisher.loaned_count(), 0);
    drop(publisher.loan(32).unwrap());
    assert_eq!(publisher.loaned_count(), 0);
    let loan = publisher.loan(32).unwrap();
    publisher.close();
    assert!(loan.is_consumed());
    assert!(loan.send().is_err());
    assert_eq!(publisher.loaned_count(), 0);
    assert!(publisher.loan(32).is_err());
}

#[test]
fn mutable_import_export_are_single_counted_copies_and_strict_rejects() {
    let (mut publisher, copies) = setup(CopyPolicy::AllowCopies, 1);
    let loan = publisher.loan(4).unwrap();
    loan.write(Buffer::from(vec![3, 4]).into(), Some(1))
        .unwrap();
    let snapshot = loan.snapshot_with(|| Ok(Buffer::from(vec![0; 4]))).unwrap();
    assert_eq!(snapshot.as_ref(), &[0, 3, 4, 0]);
    assert!(loan
        .write(Buffer::from(vec![1, 2]).into(), Some(3))
        .is_err());
    loan.fill(8).unwrap();
    loan.send().unwrap();
    assert_eq!(snapshot.as_ref(), &[0, 3, 4, 0]);
    let report = copies.report();
    let counts = report
        .entries
        .iter()
        .find(|(r, _)| *r == CopyReason::BorrowedInput)
        .unwrap()
        .1;
    assert_eq!(counts.copied_bytes, 6);
    assert_eq!(counts.completed, 2);

    let (mut strict, copies) = setup(CopyPolicy::RequireZeroCopy, 1);
    let loan = strict.loan(4).unwrap();
    assert!(loan.write(Buffer::from(vec![1]).into(), None).is_err());
    assert!(loan
        .snapshot_with(|| panic!("strict export allocated destination"))
        .is_err());
    assert!(strict.publish_raw(Buffer::from(vec![1, 2]).into()).is_err());
    loan.fill(3).unwrap();
    loan.send().unwrap();
    let counts = copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::BorrowedInput)
        .unwrap()
        .1;
    assert_eq!(counts.copied_bytes, 0);
    assert_eq!(counts.rejected_bytes, 7);
}

#[test]
fn channel_close_invalidates_existing_loans_and_publisher() {
    let (mut publisher, _) = setup(CopyPolicy::AllowCopies, 1);
    let loan = publisher.loan(4).unwrap();
    publisher.channel_open.store(false, Ordering::Release);
    assert!(!publisher.is_valid());
    assert!(loan.is_consumed());
    assert!(loan.send().is_err());
    publisher.close();
    assert_eq!(publisher.loaned_count(), 0);
}

#[test]
fn startup_failure_reaches_caller_without_leaking_credit() {
    let (mut publisher, _) = setup(CopyPolicy::AllowCopies, 1);
    publisher.channel_name = String::new();
    assert!(publisher.loan(4).is_err());
    assert_eq!(publisher.loaned_count(), 0);
    publisher.close();
}

#[test]
fn late_subscriber_receives_history_without_another_publication() {
    let (mut publisher, _) = setup(CopyPolicy::RequireZeroCopy, 1);
    let loan = publisher.loan(4).unwrap();
    loan.fill(19).unwrap();
    loan.send().unwrap();
    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service = node
        .service_builder(&ServiceName::new(&publisher.channel_name).unwrap())
        .publish_subscribe::<[u8]>()
        .max_publishers(10)
        .max_subscribers(10)
        .history_size(64)
        .subscriber_max_buffer_size(64)
        .open_or_create()
        .unwrap();
    let subscriber = service.subscriber_builder().create().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Some(sample) = subscriber.receive().unwrap() {
            assert_eq!(sample.payload(), &[19; 4]);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "late subscriber received no history"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}
