//! Native handles stay on one thread. Validated JS input is used only while
//! its synchronous callback is blocked, with no JS reentry or cancellation.
use iceoryx2::port::update_connections::UpdateConnections;
use iceoryx2::prelude::*;
use napi::{bindgen_prelude::Buffer, Error, Result};
use remotemedia_types::{CopyContext, CopyOperation, CopyReason, RuntimeData};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

type NativeLoan = iceoryx2::sample_mut::SampleMut<ipc::Service, [u8], ()>;
type Job = Box<dyn FnOnce(&mut State) + Send>;
struct State {
    publisher: iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    loans: HashMap<u64, NativeLoan>,
    next_id: u64,
    max_payload: usize,
    max_loans: usize,
    copies: CopyContext,
}
struct Endpoint {
    copies: CopyContext,
    tx: mpsc::SyncSender<Job>,
    alive: AtomicBool,
    channel_open: Arc<AtomicBool>,
    count: AtomicUsize,
    join: Mutex<Option<JoinHandle<()>>>,
}
#[derive(Clone)]
pub(in super::super) struct PublisherWorker(Arc<Endpoint>);

struct WorkerExit(Arc<Endpoint>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.0.count.store(0, Ordering::Release);
        self.0.alive.store(false, Ordering::Release);
    }
}

impl PublisherWorker {
    pub fn start(
        name: String,
        max_payload: usize,
        max_loans: usize,
        channel_open: Arc<AtomicBool>,
        copies: CopyContext,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<Job>(1);
        let endpoint = Arc::new(Endpoint {
            copies: copies.clone(),
            tx,
            alive: AtomicBool::new(true),
            channel_open,
            count: AtomicUsize::new(0),
            join: Mutex::new(None),
        });
        let thread_endpoint = endpoint.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join = std::thread::Builder::new()
            .name("node-ipc-publisher".into())
            .spawn(move || {
                let _exit = WorkerExit(thread_endpoint.clone());
                match State::open(&name, max_payload, max_loans, copies) {
                    Ok(mut state) => {
                        let _ = ready_tx.send(Ok(()));
                        state.run(rx, &thread_endpoint);
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .map_err(|e| Error::from_reason(e.to_string()))?;
        *endpoint.join.lock().unwrap() = Some(join);
        let worker = Self(endpoint);
        if let Err(error) = ready_rx
            .recv()
            .unwrap_or_else(|_| Err("publisher startup interrupted".into()))
        {
            worker.close();
            return Err(Error::from_reason(error));
        }
        Ok(worker)
    }
    pub fn is_alive(&self) -> bool {
        self.0.alive.load(Ordering::Acquire) && self.0.channel_open.load(Ordering::Acquire)
    }
    pub fn loaned_count(&self) -> u32 {
        self.0.count.load(Ordering::Acquire) as u32
    }
    pub fn close(&self) {
        self.0.alive.store(false, Ordering::Release);
        if let Some(join) = self.0.join.lock().unwrap().take() {
            let _ = join.join();
        }
        self.0.count.store(0, Ordering::Release);
    }
    /// No timeout: JS remains blocked until the operation finishes or is dropped.
    fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut State) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if !self.is_alive() {
            return Err(Error::from_reason("Publisher or channel is closed"));
        }
        let (tx, rx) = mpsc::sync_channel(1);
        let endpoint = self.0.clone();
        self.0
            .tx
            .send(Box::new(move |state| {
                let result = if endpoint.alive.load(Ordering::Acquire)
                    && endpoint.channel_open.load(Ordering::Acquire)
                {
                    operation(state)
                } else {
                    Err(Error::from_reason("Publisher or channel is closed"))
                };
                endpoint.count.store(state.loans.len(), Ordering::Release);
                let _ = tx.send(result);
            }))
            .map_err(|_| Error::from_reason("Publisher worker stopped"))?;
        rx.recv()
            .map_err(|_| Error::from_reason("Publisher worker interrupted"))?
    }
    pub fn loan(&self, size: usize) -> Result<Option<u64>> {
        self.call(move |state| {
            state.validate_size(size)?;
            if state.loans.len() >= state.max_loans {
                return Ok(None);
            }
            let id = state.next_id;
            state.next_id = id
                .checked_add(1)
                .ok_or_else(|| Error::from_reason("Loan ID exhausted"))?;
            let loan = state
                .publisher
                .loan_slice_uninit(size)
                .map_err(native_error)?
                .write_from_fn(|_| 0);
            state.loans.insert(id, loan);
            Ok(Some(id))
        })
    }
    pub fn set(&self, id: u64, index: usize, value: u8) -> Result<()> {
        self.call(move |state| {
            let slot = state
                .loan_mut(id)?
                .payload_mut()
                .get_mut(index)
                .ok_or_else(|| Error::from_reason("Loan index out of bounds"))?;
            *slot = value;
            Ok(())
        })
    }
    pub fn fill(&self, id: u64, value: u8) -> Result<()> {
        self.call(move |state| {
            state.loan_mut(id)?.payload_mut().fill(value);
            Ok(())
        })
    }
    pub fn authorize_snapshot(&self, size: u32) -> Result<CopyOperation> {
        self.0
            .copies
            .begin(CopyReason::BorrowedInput, size as u64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }
    pub fn snapshot_into(
        &self,
        id: u64,
        mut output: Buffer,
        operation: CopyOperation,
    ) -> Result<Buffer> {
        self.call(move |state| {
            let bytes = state.loan_mut(id)?.payload();
            if bytes.len() != output.len() {
                return Err(Error::from_reason(
                    "Snapshot destination has incorrect size",
                ));
            }
            output.as_mut().copy_from_slice(bytes);
            operation.complete();
            Ok(output)
        })
    }
    pub fn write(&self, id: u64, input: Buffer, offset: usize) -> Result<()> {
        // InputBuffer validated ordinary non-shared storage after all argument
        // conversions. No JS executes during call(). Return its owning reference
        // for disposal on the JS thread. Never use from async/cancellable code.
        let (input, result) = self.call(move |state| {
            let result = state.write(id, &input, offset);
            Ok((input, result))
        })?;
        drop(input);
        result
    }
    pub fn finish(&self, id: u64, send: bool) -> Result<()> {
        self.call(move |state| {
            let loan = state
                .loans
                .remove(&id)
                .ok_or_else(|| Error::from_reason("Loan already consumed"))?;
            if send {
                loan.send().map_err(native_error)?;
            }
            Ok(())
        })
    }
    pub fn publish(&self, data: RuntimeData) -> Result<()> {
        self.call(move |state| {
            remotemedia_core::multiprocess::publish_runtime_data_with_limit(
                &state.publisher,
                &data,
                &state.copies,
                state.max_payload,
            )
            .map_err(Error::from_reason)
        })
    }
    pub fn publish_raw(&self, input: Buffer) -> Result<()> {
        let (input, result) = self.call(move |state| {
            let result = state.publish_raw(&input);
            Ok((input, result))
        })?;
        drop(input);
        result
    }
}
fn native_error(error: impl std::fmt::Debug) -> Error {
    Error::from_reason(format!("Native IPC: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn worker_panic_disconnects_waiter_and_reclaims_loans() {
        let worker = PublisherWorker::start(
            format!("napi_panic_{}", std::process::id()),
            1024,
            1,
            Arc::new(AtomicBool::new(true)),
            CopyContext::new(remotemedia_types::CopyPolicy::RequireZeroCopy, "panic-test"),
        )
        .unwrap();
        worker.loan(16).unwrap();
        assert!(worker
            .call::<()>(|_| panic!("injected worker failure"))
            .is_err());
        worker.close();
        assert!(!worker.is_alive());
        assert_eq!(worker.loaned_count(), 0);
        assert!(worker.loan(4).is_err());
    }
}
impl State {
    fn run(&mut self, rx: mpsc::Receiver<Job>, endpoint: &Endpoint) {
        while endpoint.alive.load(Ordering::Acquire)
            && endpoint.channel_open.load(Ordering::Acquire)
        {
            match rx.recv_timeout(Duration::from_millis(25)) {
                Ok(job) => job(self),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Late subscribers need a connection refresh to
                    // receive history even without another publish.
                    if let Err(error) = self.publisher.update_connections() {
                        tracing::error!(?error, "Node publisher connection maintenance failed");
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn open(
        name: &str,
        max_payload: usize,
        max_loans: usize,
        copies: CopyContext,
    ) -> std::result::Result<Self, String> {
        let create = || -> Result<Self> {
            let node = NodeBuilder::new()
                .create::<ipc::Service>()
                .map_err(native_error)?;
            // Matches ChannelRegistry and the existing subscriber service contract.
            let service = node
                .service_builder(&ServiceName::new(name).map_err(native_error)?)
                .publish_subscribe::<[u8]>()
                .max_publishers(10)
                .max_subscribers(10)
                .history_size(64)
                .subscriber_max_buffer_size(64)
                .open_or_create()
                .map_err(native_error)?;
            let publisher = service
                .publisher_builder()
                .initial_max_slice_len(max_payload)
                .max_loaned_samples(max_loans.saturating_add(1))
                .allocation_strategy(AllocationStrategy::PowerOfTwo)
                .create()
                .map_err(native_error)?;
            Ok(Self {
                publisher,
                loans: HashMap::new(),
                next_id: 1,
                max_payload,
                max_loans,
                copies,
            })
        };
        create().map_err(|e| e.to_string())
    }
    fn validate_size(&self, size: usize) -> Result<()> {
        if size > self.max_payload {
            Err(Error::from_reason("Data exceeds max payload size"))
        } else {
            Ok(())
        }
    }
    fn loan_mut(&mut self, id: u64) -> Result<&mut NativeLoan> {
        self.loans
            .get_mut(&id)
            .ok_or_else(|| Error::from_reason("Loan already consumed"))
    }
    fn write(&mut self, id: u64, input: &[u8], offset: usize) -> Result<()> {
        let copies = self.copies.clone();
        let destination = self.loan_mut(id)?.payload_mut();
        let end = offset
            .checked_add(input.len())
            .filter(|end| *end <= destination.len())
            .ok_or_else(|| Error::from_reason("Write exceeds loan size"))?;
        copies
            .materialize(CopyReason::BorrowedInput, input.len() as u64, || {
                destination[offset..end].copy_from_slice(input)
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }
    fn publish_raw(&self, input: &[u8]) -> Result<()> {
        self.validate_size(input.len())?;
        let operation = self
            .copies
            .begin(CopyReason::BorrowedInput, input.len() as u64)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let sample = self
            .publisher
            .loan_slice_uninit(input.len())
            .map_err(native_error)?
            .write_from_slice(input);
        operation.complete();
        sample.send().map_err(native_error)?;
        Ok(())
    }
}
