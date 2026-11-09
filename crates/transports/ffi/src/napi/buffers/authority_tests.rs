use super::*;
use remotemedia_types::CopyCounts;

fn counts(context: &CopyContext) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}

#[test]
fn authority_policy_rejects_before_operation_and_cannot_be_relaxed() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "node-owner-strict");
    for flag in [None, Some(false), Some(true)] {
        assert!(
            materialize_with_context(Some(&context), flag, 8, || -> Result<()> {
                panic!("a strict owner must reject before allocating/copying")
            })
            .is_err()
        );
    }
    materialize_with_context(Some(&context), Some(false), 0, || Ok(())).unwrap();
    assert_eq!(counts(&context).rejected_bytes, 24);
    assert_eq!(counts(&context).copied_bytes, 0);
    assert_eq!(counts(&context).in_flight, 0);
}

#[test]
fn authority_policy_strengthening_and_failures_keep_the_originating_ledger() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "node-owner-compatible");
    materialize_with_context(Some(&context), None, 8, || Ok(())).unwrap();
    assert!(
        materialize_with_context(Some(&context), Some(true), 3, || -> Result<()> {
            panic!("a strict per-call selector must reject before the operation")
        })
        .is_err()
    );
    assert!(
        materialize_with_context(Some(&context), Some(false), 7, || -> Result<()> {
            Err(Error::from_reason("allocator failure"))
        })
        .is_err()
    );
    let report = counts(&context);
    assert_eq!(report.copied_bytes, 8);
    assert_eq!(report.rejected_bytes, 3);
    assert_eq!(report.failed_requested_bytes, 7);
    assert_eq!(report.in_flight, 0);
}
