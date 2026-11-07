use super::NodeRuntimeContext;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

const OPEN: u8 = 0;
const REQUESTED: u8 = 1;
const CLOSED: u8 = 2;

/// Owned by the tick future, never by an escaped node context. Dropping that
/// future invalidates every clone without committing a completion request.
pub(crate) struct SourceTick(Arc<AtomicU8>);

impl SourceTick {
    pub(crate) fn close(self) -> bool {
        self.0.swap(CLOSED, Ordering::AcqRel) == REQUESTED
    }
}

impl Drop for SourceTick {
    fn drop(&mut self) {
        self.0.store(CLOSED, Ordering::Release);
    }
}

impl NodeRuntimeContext {
    pub(crate) fn for_source_tick(&self) -> (Self, SourceTick) {
        let state = Arc::new(AtomicU8::new(OPEN));
        let mut context = self.clone();
        context.source_completion = Some(state.clone());
        (context, SourceTick(state))
    }

    /// Finish this source when the current tick successfully returns. Remaining
    /// outputs must still be published before returning from the tick.
    pub fn finish_source(&self) -> crate::Result<()> {
        let state = self.source_completion.as_ref().ok_or_else(|| {
            crate::Error::Execution("source completion requires an active tick context".into())
        })?;
        state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value != CLOSED).then_some(REQUESTED)
            })
            .map(|_| ())
            .map_err(|_| crate::Error::Execution("source tick context is closed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_is_scoped_to_one_successful_tick() {
        let session = NodeRuntimeContext::for_test("session", "source");
        assert!(session.finish_source().is_err());
        let (quiet, tick) = session.for_source_tick();
        assert!(!tick.close());
        assert!(quiet.finish_source().is_err());
        let (finished, tick) = session.for_source_tick();
        finished.finish_source().unwrap();
        finished.clone().finish_source().unwrap();
        assert!(tick.close());
        assert!(finished.finish_source().is_err());
        let (next, tick) = session.for_source_tick();
        assert!(!tick.close());
        assert!(next.finish_source().is_err());
    }

    #[test]
    fn dropping_tick_invalidates_retained_context() {
        let session = NodeRuntimeContext::for_test("session", "source");
        let (context, tick) = session.for_source_tick();
        context.finish_source().unwrap();
        drop(tick);
        assert!(context.finish_source().is_err());
    }
}
