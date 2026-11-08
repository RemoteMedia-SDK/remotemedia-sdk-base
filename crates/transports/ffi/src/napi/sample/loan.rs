use super::super::{buffers::InputBuffer, publisher::PublisherWorker};
use napi::{bindgen_prelude::Buffer, Env, Error, Result};
use napi_derive::napi;
use std::sync::atomic::{AtomicBool, Ordering};

/// Exclusive native shared-memory loan. No writable JS alias escapes publication.
#[napi]
pub struct LoanedSample {
    worker: PublisherWorker,
    id: u64,
    size: u32,
    consumed: AtomicBool,
}
impl LoanedSample {
    pub(in super::super) fn new(worker: PublisherWorker, id: u64, size: u32) -> Self {
        Self {
            worker,
            id,
            size,
            consumed: AtomicBool::new(false),
        }
    }
    fn check(&self) -> Result<()> {
        if self.is_consumed() {
            Err(Error::from_reason(
                "Loan already consumed or publisher closed",
            ))
        } else {
            Ok(())
        }
    }
    fn consume(&self, send: bool) -> Result<()> {
        if self.consumed.swap(true, Ordering::AcqRel) {
            return Err(Error::from_reason("Loan already consumed"));
        }
        self.worker.finish(self.id, send)
    }
    pub(in super::super) fn snapshot_with(
        &self,
        allocate: impl FnOnce() -> Result<Buffer>,
    ) -> Result<Buffer> {
        self.check()?;
        let operation = self.worker.authorize_snapshot(self.size)?;
        self.worker.snapshot_into(self.id, allocate()?, operation)
    }
}
#[napi]
impl LoanedSample {
    /// Explicit counted snapshot. Mutating it does not modify the native loan.
    /// Use set/fill or write instead. A snapshot remains valid after send/release.
    #[napi(getter)]
    pub fn buffer(&self, env: Env) -> Result<Buffer> {
        self.snapshot_with(|| super::super::buffers::output_buffer(env, self.size as usize))
    }
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.size
    }
    #[napi(getter)]
    pub fn is_consumed(&self) -> bool {
        self.consumed.load(Ordering::Acquire) || !self.worker.is_alive()
    }
    /// Generate one byte directly in native shared memory.
    #[napi]
    pub fn set(&self, index: u32, value: u32) -> Result<()> {
        self.check()?;
        let value =
            u8::try_from(value).map_err(|_| Error::from_reason("Byte value exceeds 255"))?;
        self.worker.set(self.id, index as usize, value)
    }
    /// Generate a repeated byte directly in native shared memory.
    #[napi]
    pub fn fill(&self, value: u32) -> Result<()> {
        self.check()?;
        let value =
            u8::try_from(value).map_err(|_| Error::from_reason("Byte value exceeds 255"))?;
        self.worker.fill(self.id, value)
    }
    /// Counted one-copy import from ordinary, non-shared JS Buffer storage.
    #[napi]
    pub fn write(&self, data: InputBuffer, offset: Option<u32>) -> Result<()> {
        self.check()?;
        let data = data.into_buffer()?;
        self.worker
            .write(self.id, data, offset.unwrap_or(0) as usize)
    }
    /// Actual native publication, acknowledged before returning. Consumes once,
    /// including on send failure; published storage is never writable again.
    #[napi]
    pub fn send(&self) -> Result<()> {
        self.consume(true)
    }
    #[napi]
    pub fn release(&self) -> Result<()> {
        self.consume(false)
    }
}
impl Drop for LoanedSample {
    fn drop(&mut self) {
        if !self.consumed.swap(true, Ordering::AcqRel) && self.worker.is_alive() {
            let _ = self.worker.finish(self.id, false);
        }
    }
}
