//! Native receive ownership shared by pull, asynchronous and callback APIs.
mod callback;
pub(super) mod native;
use super::sample::ReceivedSample;
use callback::CallbackHandle;
use napi::{Env, Error, JsFunction, Result};
use napi_derive::napi;
use native::NativeReceiver;
use remotemedia_types::CopyContext;
use std::sync::{atomic::AtomicBool, Arc, Mutex};

#[napi]
pub struct NapiSubscriber {
    channel_name: String,
    buffer_size: u32,
    receiver: Arc<NativeReceiver>,
    callback: Mutex<Option<Arc<CallbackHandle>>>,
}
impl NapiSubscriber {
    pub(crate) fn new(
        channel_name: String,
        buffer_size: usize,
        channel_open: Arc<AtomicBool>,
        copies: CopyContext,
    ) -> Result<Self> {
        let receiver = NativeReceiver::open(&channel_name, buffer_size, channel_open, copies)?;
        Ok(Self {
            channel_name,
            buffer_size: buffer_size as u32,
            receiver: Arc::new(receiver),
            callback: Mutex::new(None),
        })
    }
    async fn wait(&self, timeout: Option<u32>) -> Result<Option<ReceivedSample>> {
        let deadline = timeout
            .map(|ms| tokio::time::Instant::now() + std::time::Duration::from_millis(ms as u64));
        loop {
            if let Some(sample) = self.receive()? {
                return Ok(Some(sample));
            }
            if deadline.is_some_and(|end| tokio::time::Instant::now() >= end) {
                return Ok(None);
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}
#[napi]
impl NapiSubscriber {
    #[napi(getter)]
    pub fn channel_name(&self) -> String {
        self.channel_name.clone()
    }
    #[napi(getter)]
    pub fn buffer_size(&self) -> u32 {
        self.buffer_size
    }
    #[napi(getter)]
    pub fn is_valid(&self) -> bool {
        self.receiver.is_open()
    }
    /// Pending JavaScript callback deliveries, excluding the native receive queue.
    #[napi(getter)]
    pub fn pending_count(&self) -> u32 {
        self.callback
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |callback| callback.pending_count())
    }
    /// Native samples retained by wrappers, slices, callbacks or pipeline owners.
    #[napi(getter)]
    pub fn borrowed_count(&self) -> u32 {
        self.receiver.borrowed_count() as u32
    }
    #[napi(getter)]
    pub fn max_borrowed_samples(&self) -> u32 {
        self.receiver.max_borrowed() as u32
    }
    /// Returns null when empty or when retained owners occupy all borrow slots.
    #[napi]
    pub fn receive(&self) -> Result<Option<ReceivedSample>> {
        let callback = self.callback.lock().unwrap();
        if callback
            .as_ref()
            .is_some_and(|callback| callback.is_active())
        {
            return Err(Error::from_reason(
                "Unsubscribe the callback before using pull receive",
            ));
        }
        self.receiver
            .receive()
            .map(|sample| sample.map(ReceivedSample::new))
    }
    #[napi]
    pub async fn receive_timeout(&self, timeout_ms: u32) -> Result<Option<ReceivedSample>> {
        self.wait(Some(timeout_ms)).await
    }
    #[napi]
    pub async fn receive_async(&self) -> Result<ReceivedSample> {
        self.wait(None)
            .await?
            .ok_or_else(|| Error::from_reason("Receive interrupted"))
    }
    /// Bounded owning callback delivery. A terminal receive error invokes
    /// callback(null, error); normal delivery invokes callback(sample, null).
    #[napi(ts_return_type = "() => void")]
    pub fn on_data(
        &self,
        env: Env,
        #[napi(ts_arg_type = "(sample: ReceivedSample | null, error: Error | null) => void")]
        callback: JsFunction,
    ) -> Result<JsFunction> {
        let mut slot = self.callback.lock().unwrap();
        if slot.as_ref().is_some_and(|callback| callback.is_active()) {
            return Err(Error::from_reason(
                "A receive callback is already registered",
            ));
        }
        if let Some(previous) = slot.take() {
            previous.stop();
        }
        let callback = CallbackHandle::start(env, callback, self.receiver.clone())?;
        let unsubscribe = callback.clone();
        let function = env.create_function_from_closure("unsubscribe", move |ctx| {
            unsubscribe.stop();
            ctx.env.get_undefined()
        })?;
        *slot = Some(callback);
        Ok(function)
    }
    #[napi]
    pub fn unsubscribe(&self) {
        let callback = self.callback.lock().unwrap().take();
        if let Some(callback) = callback {
            callback.stop();
        }
    }
    /// Stops delivery and releases queued samples; returned owners remain valid.
    #[napi]
    pub fn close(&self) {
        self.unsubscribe();
        self.receiver.close();
    }
}
impl Drop for NapiSubscriber {
    fn drop(&mut self) {
        self.close();
    }
}
