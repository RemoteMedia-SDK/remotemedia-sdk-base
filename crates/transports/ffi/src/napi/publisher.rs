//! Acknowledged commands to the thread owning the native publisher and loans.
mod worker;
use super::{buffers::InputBuffer, pipeline::NapiRuntimeData, sample::LoanedSample};
use napi_derive::napi;
use remotemedia_types::CopyContext;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
pub(super) use worker::PublisherWorker;

#[napi]
pub struct NapiPublisher {
    channel_name: String,
    max_payload_size: usize,
    max_loans: usize,
    channel_open: Arc<AtomicBool>,
    copies: CopyContext,
    worker: Option<PublisherWorker>,
    closed: bool,
}
impl NapiPublisher {
    pub(crate) fn new(
        channel_name: String,
        max_payload_size: usize,
        max_loans: usize,
        channel_open: Arc<AtomicBool>,
        copies: CopyContext,
    ) -> napi::Result<Self> {
        if max_loans == 0 || max_payload_size == 0 {
            return Err(napi::Error::from_reason(
                "payload and loan limits must be positive",
            ));
        }
        Ok(Self {
            channel_name,
            max_payload_size,
            max_loans,
            channel_open,
            copies,
            worker: None,
            closed: false,
        })
    }
    fn ensure_worker(&mut self) -> napi::Result<&PublisherWorker> {
        if !self.is_valid() {
            return Err(napi::Error::from_reason("Publisher or channel is closed"));
        }
        if self.worker.is_none() {
            self.worker = Some(PublisherWorker::start(
                self.channel_name.clone(),
                self.max_payload_size,
                self.max_loans,
                self.channel_open.clone(),
                self.copies.clone(),
            )?);
        }
        Ok(self.worker.as_ref().expect("worker initialized"))
    }
}
#[napi]
impl NapiPublisher {
    #[napi(getter)]
    pub fn channel_name(&self) -> String {
        self.channel_name.clone()
    }
    #[napi(getter)]
    pub fn is_valid(&self) -> bool {
        !self.closed
            && self.channel_open.load(Ordering::Acquire)
            && self.worker.as_ref().map_or(true, PublisherWorker::is_alive)
    }
    #[napi(getter)]
    pub fn loaned_count(&self) -> u32 {
        self.worker
            .as_ref()
            .map_or(0, PublisherWorker::loaned_count)
    }
    #[napi(getter)]
    pub fn max_loans(&self) -> u32 {
        self.max_loans as u32
    }
    /// Allocate a zero-initialized native loan. Use set/fill for direct writes.
    #[napi]
    pub fn loan(&mut self, size: u32) -> napi::Result<LoanedSample> {
        self.try_loan(size)?
            .ok_or_else(|| napi::Error::from_reason("Loan pool exhausted"))
    }
    /// Returns null at the outstanding-loan limit. Commands acknowledge synchronously.
    #[napi]
    pub fn try_loan(&mut self, size: u32) -> napi::Result<Option<LoanedSample>> {
        let worker = self.ensure_worker()?;
        worker
            .loan(size as usize)
            .map(|id| id.map(|id| LoanedSample::new(worker.clone(), id, size)))
    }
    /// Counted MessagePack serialization directly into a native loan.
    #[napi]
    pub fn publish(&mut self, data: &NapiRuntimeData) -> napi::Result<()> {
        self.ensure_worker()?.publish(data.get_inner().clone())
    }
    /// Copy a validated mutable JS buffer once into its final native destination.
    #[napi]
    pub fn publish_raw(&mut self, data: InputBuffer) -> napi::Result<()> {
        let data = data.into_buffer()?;
        self.ensure_worker()?.publish_raw(data)
    }
    /// Joins the worker and invalidates outstanding loans.
    #[napi]
    pub fn close(&mut self) {
        self.closed = true;
        if let Some(worker) = self.worker.take() {
            worker.close();
        }
    }
}
impl Drop for NapiPublisher {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
