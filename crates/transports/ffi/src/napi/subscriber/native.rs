//! Owning native receives. The thread-safe service uses the same IPC storage
//! and wire contract as ipc::Service; only its local synchronization differs.
use iceoryx2::prelude::*;
use napi::{Error, Result};
use remotemedia_types::{CopyContext, SharedBytes};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

type NativeSubscriber = iceoryx2::port::subscriber::Subscriber<ipc_threadsafe::Service, [u8], ()>;
type NativeSample = iceoryx2::sample::Sample<ipc_threadsafe::Service, [u8], ()>;

struct Credit(Arc<AtomicUsize>);
impl Drop for Credit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The sample is private and used only for immutable payload access. Native
/// sample Drop releases its offset before Credit returns our admission slot.
struct NativePayload {
    sample: NativeSample,
    _credit: Credit,
}
// SAFETY: iceoryx provides Send for this Sample with its mutex-protected IPC
// service, but no blanket Sync for arbitrary sample payload types. This wrapper
// is restricted to immutable [u8] and exposes only payload(). The pointer, length
// and bytes do not change while the sample is retained. No native handle or
// mutable payload escapes. Rust/SharedBytes ownership excludes Drop while any
// payload reference is live; Drop uses the thread-safe subscriber state.
unsafe impl Sync for NativePayload {}
impl AsRef<[u8]> for NativePayload {
    fn as_ref(&self) -> &[u8] {
        self.sample.payload()
    }
}

pub(in super::super) struct NativeReceived {
    pub bytes: SharedBytes,
    pub copies: CopyContext,
    pub timestamp_ns: i64,
}

pub(super) struct NativeReceiver {
    subscriber: Mutex<Option<NativeSubscriber>>,
    channel_open: Arc<AtomicBool>,
    closed: AtomicBool,
    borrowed: Arc<AtomicUsize>,
    max_borrowed: usize,
    copies: CopyContext,
}

impl NativeReceiver {
    pub fn open(
        name: &str,
        buffer_size: usize,
        channel_open: Arc<AtomicBool>,
        copies: CopyContext,
    ) -> Result<Self> {
        if buffer_size == 0 {
            return Err(Error::from_reason("Receive buffer size must be positive"));
        }
        if !channel_open.load(Ordering::Acquire) {
            return Err(Error::from_reason("Channel is closed"));
        }
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(native_error)?;
        let service = node
            .service_builder(&ServiceName::new(name).map_err(native_error)?)
            .publish_subscribe::<[u8]>()
            .max_publishers(10)
            .max_subscribers(10)
            .history_size(64)
            .subscriber_max_buffer_size(64)
            .open_or_create()
            .map_err(native_error)?;
        let max_borrowed = service
            .static_config()
            .subscriber_max_borrowed_samples()
            .min(buffer_size);
        let subscriber = service
            .subscriber_builder()
            .buffer_size(buffer_size)
            .create()
            .map_err(native_error)?;
        Ok(Self {
            subscriber: Mutex::new(Some(subscriber)),
            channel_open,
            closed: AtomicBool::new(false),
            borrowed: Arc::new(AtomicUsize::new(0)),
            max_borrowed,
            copies,
        })
    }
    pub fn is_open(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && self.channel_open.load(Ordering::Acquire)
    }
    pub fn borrowed_count(&self) -> usize {
        self.borrowed.load(Ordering::Acquire)
    }
    pub fn max_borrowed(&self) -> usize {
        self.max_borrowed
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.subscriber.lock().unwrap().take();
    }
    /// Retained samples apply backpressure by stopping further native borrows.
    /// The native bounded receive queue keeps its negotiated overflow behavior.
    pub fn receive(&self) -> Result<Option<NativeReceived>> {
        let subscriber = self.subscriber.lock().unwrap();
        if !self.is_open() {
            return Err(Error::from_reason("Subscriber or channel is closed"));
        }
        if self.borrowed_count() >= self.max_borrowed {
            return Ok(None);
        }
        let Some(sample) = subscriber
            .as_ref()
            .ok_or_else(|| Error::from_reason("Subscriber is closed"))?
            .receive()
            .map_err(native_error)?
        else {
            return Ok(None);
        };
        if sample.payload().len() > u32::MAX as usize {
            return Err(Error::from_reason(
                "Native sample exceeds Node IPC size limit",
            ));
        }
        self.borrowed.fetch_add(1, Ordering::AcqRel);
        let bytes = SharedBytes::from_owner(NativePayload {
            sample,
            _credit: Credit(self.borrowed.clone()),
        });
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        Ok(Some(NativeReceived {
            bytes,
            copies: self.copies.clone(),
            timestamp_ns,
        }))
    }
}
fn native_error(error: impl std::fmt::Debug) -> Error {
    Error::from_reason(format!("Native receive: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::napi::publisher::NapiPublisher;
    use remotemedia_types::CopyPolicy;

    fn setup(limit: usize) -> (NapiPublisher, NativeReceiver) {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "napi_owned_recv_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let open = Arc::new(AtomicBool::new(true));
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, &name);
        let publisher =
            NapiPublisher::new(name.clone(), 1024, 4, open.clone(), copies.clone()).unwrap();
        let receiver = NativeReceiver::open(&name, limit, open, copies).unwrap();
        (publisher, receiver)
    }
    fn send(publisher: &mut NapiPublisher, value: u32) {
        let loan = publisher.loan(8).unwrap();
        loan.fill(value).unwrap();
        loan.send().unwrap();
    }
    #[test]
    fn native_owner_keeps_storage_and_credit_across_threads_and_close() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NativeReceiver>();
        assert_send_sync::<SharedBytes>();
        let (mut publisher, receiver) = setup(1);
        send(&mut publisher, 11);
        let received = receiver.receive().unwrap().unwrap();
        let pointer = received.bytes.as_slice().as_ptr();
        let view = received.bytes.slice(2..6).unwrap();
        assert!(view.shares_storage(&received.bytes));
        assert_eq!(unsafe { pointer.add(2) }, view.as_slice().as_ptr());
        assert!(received.timestamp_ns > 1_000_000_000);
        assert!(received
            .copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.copied_bytes == 0));
        send(&mut publisher, 22);
        assert!(receiver.receive().unwrap().is_none());
        drop(received);
        assert_eq!(receiver.borrowed_count(), 1);
        publisher.close();
        receiver.close();
        let join = std::thread::spawn(move || {
            assert_eq!(view.as_slice(), &[11; 4]);
            view
        });
        let view = join.join().unwrap();
        assert_eq!(view.as_slice().as_ptr(), unsafe { pointer.add(2) });
        drop(view);
        assert_eq!(receiver.borrowed_count(), 0);
        assert!(receiver.receive().is_err());
    }
    #[test]
    fn final_owner_release_allows_next_native_borrow() {
        let (mut publisher, receiver) = setup(1);
        assert_eq!(receiver.max_borrowed(), 1);
        send(&mut publisher, 1);
        let first = receiver.receive().unwrap().unwrap();
        send(&mut publisher, 2);
        assert!(receiver.receive().unwrap().is_none());
        let alias = first.bytes.clone();
        drop(first);
        assert!(receiver.receive().unwrap().is_none());
        drop(alias);
        let second = receiver.receive().unwrap().unwrap();
        assert_eq!(second.bytes.as_slice(), &[2; 8]);
    }
}
