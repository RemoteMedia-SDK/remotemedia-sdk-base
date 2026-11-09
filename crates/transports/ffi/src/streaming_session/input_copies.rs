//! Input-selector reports that also account to the owning core session.
use remotemedia_types::{
    CopyContext, CopyCounts, CopyEvent, CopyLedger, CopyPolicy, CopyReason, COPY_REASONS,
};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct InputCopies {
    pub(super) session: CopyContext,
    pub(super) inherited: CopyContext,
    pub(super) strict: CopyContext,
}

impl InputCopies {
    pub(super) fn new(session: CopyContext) -> Self {
        let input = |policy| {
            CopyContext::from_ledger(
                policy,
                session.boundary(),
                Arc::new(InputLedger {
                    session: session.clone(),
                    local: CopyContext::new(policy, session.boundary()),
                }),
            )
        };
        Self {
            inherited: input(session.policy()),
            strict: input(CopyPolicy::RequireZeroCopy),
            session,
        }
    }
}

/// Retain the exact originating authority. A selector can only inherit or
/// strengthen its policy; every event is also recorded in that session ledger.
/// The local report preserves the existing per-selector API. The two reports
/// overlap, and snapshots across ledgers are only comparable after quiescence.
struct InputLedger {
    session: CopyContext,
    local: CopyContext,
}

impl CopyLedger for InputLedger {
    fn counts(&self) -> [CopyCounts; COPY_REASONS.len()] {
        self.local.counts()
    }

    fn record(&self, reason: CopyReason, event: CopyEvent) {
        self.session.record(reason, event);
        self.local.record(reason, event);
    }
}
