//! A bounded owning queue with payload-free Node-API wakeups. Unlike passing
//! sample owners through napi-rs 2.16's TSFN data boxes, abort/env teardown can
//! reclaim every queued native owner, including when JS callbacks are unavailable.
use super::native::{NativeReceived, NativeReceiver};
use crate::napi::sample::ReceivedSample;
use napi::{
    bindgen_prelude::ToNapiValue, sys, Env, Error, JsError, JsFunction, NapiRaw, Result, Status,
};
use std::{
    collections::VecDeque,
    ffi::c_void,
    ptr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

struct RawHandle(sys::napi_threadsafe_function);
// SAFETY: this handle is accessed only through a mutex, and only Node's explicitly
// thread-safe call/release operations run off the JS thread. Creation is local.
unsafe impl Send for RawHandle {}

struct State {
    queue: Mutex<VecDeque<std::result::Result<NativeReceived, String>>>,
    stopped: AtomicBool,
    notified: AtomicBool,
    raw: Mutex<Option<RawHandle>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
pub(super) struct CallbackHandle(Arc<State>);

impl CallbackHandle {
    pub fn start(
        env: Env,
        callback: JsFunction,
        receiver: Arc<NativeReceiver>,
    ) -> Result<Arc<Self>> {
        if !receiver.is_open() {
            return Err(Error::from_reason("Subscriber or channel is closed"));
        }
        let state = Arc::new(State {
            queue: Mutex::new(VecDeque::new()),
            stopped: AtomicBool::new(false),
            notified: AtomicBool::new(false),
            raw: Mutex::new(None),
            thread: Mutex::new(None),
        });
        state.create(env, callback)?;
        let handle = Arc::new(Self(state.clone()));
        let thread_state = state.clone();
        let thread = std::thread::Builder::new()
            .name("node-ipc-receive".into())
            .spawn(move || thread_state.poll(receiver))
            .map_err(|error| Error::from_reason(error.to_string()))?;
        *state.thread.lock().unwrap() = Some(thread);
        Ok(handle)
    }
    pub fn is_active(&self) -> bool {
        !self.0.stopped.load(Ordering::Acquire)
    }
    pub fn pending_count(&self) -> u32 {
        self.0.queue.lock().unwrap().len() as u32
    }
    pub fn stop(&self) {
        self.0.stop();
    }
}
impl Drop for CallbackHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

impl State {
    fn create(self: &Arc<Self>, env: Env, callback: JsFunction) -> Result<()> {
        let name = env.create_string("RemoteMedia native IPC receive")?;
        let context = Box::into_raw(Box::new(self.clone())).cast::<c_void>();
        let mut raw = ptr::null_mut();
        // SAFETY: env and callback belong to this JS thread. The boxed Arc is
        // owned by Node after success, and reclaimed exactly once by finalize.
        // Wakeups carry null data, so rejection/abort cannot leak sample owners.
        let status = unsafe {
            sys::napi_create_threadsafe_function(
                env.raw(),
                callback.raw(),
                ptr::null_mut(),
                name.raw(),
                1,
                1,
                context,
                Some(finalize),
                context,
                Some(call_js),
                &mut raw,
            )
        };
        if status != sys::Status::napi_ok {
            // SAFETY: failed creation did not adopt our context.
            unsafe {
                drop(Box::from_raw(context.cast::<Arc<State>>()));
            }
            return Err(Error::new(
                Status::from(status),
                "Cannot create receive callback",
            ));
        }
        *self.raw.lock().unwrap() = Some(RawHandle(raw));
        Ok(())
    }
    fn poll(&self, receiver: Arc<NativeReceiver>) {
        while !self.stopped.load(Ordering::Acquire) {
            match receiver.receive() {
                Ok(Some(sample)) => {
                    self.queue.lock().unwrap().push_back(Ok(sample));
                    self.notify();
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(2)),
                Err(error) => {
                    self.queue.lock().unwrap().push_back(Err(error.reason));
                    self.notify();
                    break;
                }
            }
        }
    }
    fn notify(&self) {
        let mut handle = self.raw.lock().unwrap();
        if self.stopped.load(Ordering::Acquire)
            || self.queue.lock().unwrap().is_empty()
            || self.notified.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let Some(raw) = handle.as_ref() else {
            return;
        };
        // SAFETY: the mutex excludes release, and one coalesced wakeup fits the
        // bounded queue. No heap pointer is transferred as per-call data.
        let status = unsafe {
            sys::napi_call_threadsafe_function(
                raw.0,
                ptr::null_mut(),
                sys::ThreadsafeFunctionCallMode::nonblocking,
            )
        };
        if status != sys::Status::napi_ok {
            self.stopped.store(true, Ordering::Release);
            self.queue.lock().unwrap().clear();
            let raw = handle.take().unwrap();
            // napi_closing consumes the calling thread's reference. It must
            // never be released again. Other errors leave our reference intact.
            if status != sys::Status::napi_closing {
                unsafe {
                    sys::napi_release_threadsafe_function(
                        raw.0,
                        sys::ThreadsafeFunctionReleaseMode::abort,
                    );
                }
            }
        }
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.queue.lock().unwrap().clear();
        if let Some(raw) = self.raw.lock().unwrap().take() {
            // SAFETY: this consumes the one initial TSFN reference exactly
            // once, including environment teardown. The mutex excludes calls;
            // a call that returned napi_closing already removed the handle.
            unsafe {
                sys::napi_release_threadsafe_function(
                    raw.0,
                    sys::ThreadsafeFunctionReleaseMode::abort,
                );
            }
        }
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        // A receive already in progress may have queued after the first clear.
        self.queue.lock().unwrap().clear();
    }
}

unsafe extern "C" fn finalize(_env: sys::napi_env, data: *mut c_void, _hint: *mut c_void) {
    // SAFETY: Node invokes the finalizer once for the Box adopted by create.
    // It may drain null-environment wakeups AFTER this finalizer; those carry
    // no data and must not dereference the now-freed context (see call_js).
    let state = unsafe { Box::from_raw(data.cast::<Arc<State>>()) };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.stop()));
}

unsafe extern "C" fn call_js(
    env: sys::napi_env,
    callback: sys::napi_value,
    context: *mut c_void,
    _data: *mut c_void,
) {
    // Node may already have finalized context before draining these payload-free
    // wakeups. Finalization releases the owning queue; do not access context here.
    if env.is_null() || callback.is_null() {
        return;
    }
    // SAFETY: a live JS callback runs before finalization on the same JS thread.
    let state = unsafe { &*context.cast::<Arc<State>>() };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        dispatch(env, callback, state)
    }));
    let error = match outcome {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => Error::from_reason("Native receive callback panicked"),
    };
    state.stop();
    if !env.is_null() {
        // SAFETY: we are inside Node's callback scope on its JS thread. Preserve
        // a JS exception if present; otherwise surface the conversion failure.
        unsafe {
            let mut exception = ptr::null_mut();
            let status = sys::napi_get_and_clear_last_exception(env, &mut exception);
            if status != sys::Status::napi_ok || error.status != Status::PendingException {
                exception = JsError::from(error).into_value(env);
            }
            sys::napi_fatal_exception(env, exception);
        }
    }
}

unsafe fn dispatch(env: sys::napi_env, callback: sys::napi_value, state: &State) -> Result<()> {
    state.notified.store(false, Ordering::Release);
    if state.stopped.load(Ordering::Acquire) {
        state.queue.lock().unwrap().clear();
        return Ok(());
    }
    let item = state.queue.lock().unwrap().pop_front();
    if let Some(item) = item {
        let terminal = item.is_err();
        let mut null = ptr::null_mut();
        let status = unsafe { sys::napi_get_null(env, &mut null) };
        if status != sys::Status::napi_ok {
            return Err(Error::new(
                Status::from(status),
                "Cannot create callback arguments",
            ));
        }
        let args = match item {
            Ok(received) => [
                unsafe { ReceivedSample::to_napi_value(env, ReceivedSample::new(received))? },
                null,
            ],
            Err(message) => [null, unsafe {
                JsError::from(Error::from_reason(message)).into_value(env)
            }],
        };
        let mut result = ptr::null_mut();
        let status = unsafe {
            sys::napi_call_function(env, null, callback, args.len(), args.as_ptr(), &mut result)
        };
        if status != sys::Status::napi_ok {
            return Err(Error::new(Status::from(status), "Receive callback failed"));
        }
        if terminal {
            state.stop();
            return Ok(());
        }
    }
    state.notify();
    Ok(())
}
