//! HTTP session ownership: input requests never hold the output-drain lock.
use crate::codec;
use bytes::Bytes;
use futures::StreamExt;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::{
    session_control::{ControlAddress, SYSTEM_PORT},
    PipelineExecutor, SessionHandle as CoreSession, SessionInputSender, TransportData,
};
use remotemedia_core::Result;
use remotemedia_types::CopyContext;
use std::sync::Arc;
use tokio::sync::{broadcast, watch, Mutex};
use tokio_stream::wrappers::ReceiverStream;

pub(super) type Completion = Option<std::result::Result<(), String>>;

/// Cancellation of an HTTP handler still requests and awaits core cleanup.
pub(super) struct Guard(Option<CoreSession>);
impl Guard {
    pub fn new(session: CoreSession) -> Self {
        Self(Some(session))
    }
    pub fn core(&mut self) -> &mut CoreSession {
        self.0.as_mut().unwrap()
    }
    pub async fn close(mut self) -> Result<()> {
        close_core(self.0.take().unwrap()).await
    }
}
async fn close_core(mut session: CoreSession) -> Result<()> {
    let closed = session.close().await;
    let joined = session.wait().await;
    closed.and(joined)
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let (Some(session), Ok(runtime)) = (self.0.take(), tokio::runtime::Handle::try_current())
        {
            runtime.spawn(async move {
                if let Err(error) = close_core(session).await {
                    tracing::warn!(%error, "abandoned HTTP session cleanup failed");
                }
            });
        }
    }
}

pub(super) struct HttpSession {
    input: SessionInputSender,
    pub copies: CopyContext,
    pub output: broadcast::Sender<Bytes>,
    pub completion: watch::Receiver<Completion>,
    stop: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
}

impl HttpSession {
    pub fn start(session: CoreSession, executor: &PipelineExecutor) -> Result<Arc<Self>> {
        let mut guard = Guard::new(session);
        let errors = subscribe_errors(executor, guard.core())?;
        let copies = guard.core().transport_copy_context();
        let input = guard
            .core()
            .input_sender()
            .ok_or_else(|| codec::error("session input closed"))?;
        let receivers = guard
            .core()
            .take_output_receivers()
            .ok_or_else(|| codec::error("session outputs unavailable"))?;
        let outputs = futures::stream::select_all([
            ReceiverStream::new(receivers.audio_rx),
            ReceiverStream::new(receivers.video_rx),
            ReceiverStream::new(receivers.data_rx),
        ]);
        let (output, _) = broadcast::channel(100);
        let (stop, stopped) = watch::channel(false);
        let (finished, completion) = watch::channel(None);
        let drainer = Drain {
            guard,
            outputs,
            errors,
        };
        let task = tokio::spawn(drainer.run(output.clone(), stopped, finished));
        Ok(Arc::new(Self {
            input,
            copies,
            output,
            completion,
            stop,
            task: Mutex::new(Some(task)),
        }))
    }

    pub fn input(&self) -> Result<SessionInputSender> {
        if *self.stop.borrow() || self.completion.borrow().is_some() {
            return Err(codec::error("HTTP session is closed"));
        }
        Ok(self.input.clone())
    }

    pub async fn close(&self) -> Result<()> {
        let _ = self.stop.send(true);
        let mut task = self.task.lock().await;
        if let Some(join) = task.as_mut() {
            // Keep the handle in the mutex if this close future is cancelled.
            // A later close must still join, not mistake a taken task for completion.
            let result = join.await.map_err(codec::error);
            task.take();
            result?
        } else {
            self.completion
                .borrow()
                .clone()
                .unwrap_or_else(|| {
                    Err("HTTP output drainer ended without a completion report".into())
                })
                .map_err(codec::error)
        }
    }
}
impl Drop for HttpSession {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

struct Drain {
    guard: Guard,
    outputs: futures::stream::SelectAll<ReceiverStream<TransportData>>,
    errors: broadcast::Receiver<RuntimeData>,
}

impl Drain {
    async fn run(
        mut self,
        output: broadcast::Sender<Bytes>,
        mut stopped: watch::Receiver<bool>,
        finished: watch::Sender<Completion>,
    ) -> Result<()> {
        let copies = self.guard.core().transport_copy_context();
        let result = loop {
            tokio::select! {
                _ = stopped.changed() => break Ok(()),
                error = failure(&mut self.errors) => break Err(error),
                data = self.outputs.next() => {
                    let Some(data) = data else { break Ok(()); };
                    // Metadata carries the producing participant (provenance).
                    let payload = data.data.bulk_payload_bytes();
                    match codec::encode(&data, payload, &copies) {
                        Ok(wire) => { let _ = output.send(Bytes::from(wire)); }
                        Err(error) => break Err(error),
                    }
                }
            }
        };
        // Release output receivers before joining, so bounded output cannot deadlock.
        drop(self.outputs);
        let cleanup = self.guard.close().await;
        let result = result.and(cleanup);
        finished.send_replace(Some(
            result.as_ref().map(|_| ()).map_err(ToString::to_string),
        ));
        result
    }
}

/// Attach before accepting HTTP inputs: core dispatch failures are non-terminal
/// system events, so waiting only for media EOF would leave the client hanging.
pub(super) fn subscribe_errors(
    executor: &PipelineExecutor,
    session: &CoreSession,
) -> Result<broadcast::Receiver<RuntimeData>> {
    executor
        .control_bus()
        .get(&session.session_id)
        .ok_or_else(|| codec::error("HTTP session control unavailable"))?
        .subscribe(&ControlAddress::node_out(SYSTEM_PORT))
}

pub(super) async fn failure(
    errors: &mut broadcast::Receiver<RuntimeData>,
) -> remotemedia_core::Error {
    loop {
        match errors.recv().await {
            Ok(RuntimeData::Json(event)) if event["kind"] == "node_error" => {
                return codec::error(format!(
                    "Node {} failed: {}",
                    event["node"].as_str().unwrap_or("unknown"),
                    event["message"].as_str().unwrap_or("unknown node error"),
                ));
            }
            Ok(_) => {}
            Err(error) => return codec::error(format!("HTTP session error subscription: {error}")),
        }
    }
}
