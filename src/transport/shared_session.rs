//! Shared pipeline sessions for multi-transport ingress.
//!
//! A shared session owns one core [`SessionHandle`] and exposes cloneable
//! participant ingress plus broadcast output subscriptions. Transport adapters
//! can use this to attach WebRTC, telephony, HTTP, gRPC, or FFI clients to the
//! same pipeline session without each transport creating its own router.

use crate::data::RuntimeData;
use crate::transport::data::Participant;
use crate::transport::executor::{ParticipantSessionHandle, SessionHandle, SessionInputSender};
use crate::transport::session_router::{ClientOutputReceivers, DEFAULT_ROUTER_OUTPUT_CAPACITY};
use crate::transport::StreamSession;
use crate::transport::TransportData;
use crate::{Error, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};

/// Broadcast output receivers for one subscriber attached to a shared session.
pub struct SharedPipelineOutputReceivers {
    pub audio_rx: broadcast::Receiver<TransportData>,
    pub video_rx: broadcast::Receiver<TransportData>,
    pub data_rx: broadcast::Receiver<TransportData>,
    audio_open: bool,
    video_open: bool,
    data_open: bool,
}

impl SharedPipelineOutputReceivers {
    fn new(
        audio_rx: broadcast::Receiver<TransportData>,
        video_rx: broadcast::Receiver<TransportData>,
        data_rx: broadcast::Receiver<TransportData>,
    ) -> Self {
        Self {
            audio_rx,
            video_rx,
            data_rx,
            audio_open: true,
            video_open: true,
            data_open: true,
        }
    }

    /// Receive the next output across audio, video, and data channels.
    pub async fn recv_output(&mut self) -> Result<Option<TransportData>> {
        loop {
            if !self.audio_open && !self.video_open && !self.data_open {
                return Ok(None);
            }

            tokio::select! {
                result = self.audio_rx.recv(), if self.audio_open => {
                    match result {
                        Ok(data) => return Ok(Some(data)),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => self.audio_open = false,
                    }
                }
                result = self.video_rx.recv(), if self.video_open => {
                    match result {
                        Ok(data) => return Ok(Some(data)),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => self.video_open = false,
                    }
                }
                result = self.data_rx.recv(), if self.data_open => {
                    match result {
                        Ok(data) => return Ok(Some(data)),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => self.data_open = false,
                    }
                }
            }
        }
    }

    /// Split into per-kind broadcast receivers for transports that already
    /// have optimized fanout/draining loops.
    pub fn split(
        self,
    ) -> (
        broadcast::Receiver<TransportData>,
        broadcast::Receiver<TransportData>,
        broadcast::Receiver<TransportData>,
    ) {
        (self.audio_rx, self.video_rx, self.data_rx)
    }
}

/// One core pipeline session shared by multiple transport clients.
pub struct SharedPipelineSession {
    key: String,
    session_id: String,
    input_sender: SessionInputSender,
    session_handle: Arc<Mutex<SessionHandle>>,
    audio_tx: broadcast::Sender<TransportData>,
    video_tx: broadcast::Sender<TransportData>,
    data_tx: broadcast::Sender<TransportData>,
    closed: AtomicBool,
}

/// StreamSession adapter for one participant joined to a shared pipeline.
pub struct SharedPipelineStreamSession {
    participant: ParticipantSessionHandle,
    outputs: SharedPipelineOutputReceivers,
    closed: bool,
}

impl SharedPipelineStreamSession {
    pub fn new(shared: &Arc<SharedPipelineSession>, participant: Participant) -> Self {
        Self {
            participant: shared.participant_handle(participant),
            outputs: shared.subscribe_outputs(),
            closed: false,
        }
    }
}

#[async_trait::async_trait]
impl StreamSession for SharedPipelineStreamSession {
    fn session_id(&self) -> &str {
        self.participant.session_id()
    }

    async fn send_input(&mut self, data: TransportData) -> Result<()> {
        if self.closed {
            return Err(Error::Execution(format!(
                "shared participant stream for session {} is closed",
                self.session_id()
            )));
        }
        self.participant.send(data).await
    }

    async fn recv_output(&mut self) -> Result<Option<TransportData>> {
        self.outputs.recv_output().await
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn is_active(&self) -> bool {
        !self.closed
    }
}

impl SharedPipelineSession {
    /// Wrap a newly-created core session and start draining its outputs.
    pub fn new(key: impl Into<String>, mut session_handle: SessionHandle) -> Result<Arc<Self>> {
        let key = key.into();
        let session_id = session_handle.session_id.clone();
        let input_sender = session_handle.input_sender().ok_or_else(|| {
            Error::Execution("shared session input is already closed".to_string())
        })?;
        let receivers = session_handle.take_output_receivers().ok_or_else(|| {
            Error::Execution("shared session output receivers already taken".to_string())
        })?;

        let (audio_tx, _) = broadcast::channel(DEFAULT_ROUTER_OUTPUT_CAPACITY);
        let (video_tx, _) = broadcast::channel(DEFAULT_ROUTER_OUTPUT_CAPACITY);
        let (data_tx, _) = broadcast::channel(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let shared = Arc::new(Self {
            key,
            session_id,
            input_sender,
            session_handle: Arc::new(Mutex::new(session_handle)),
            audio_tx,
            video_tx,
            data_tx,
            closed: AtomicBool::new(false),
        });

        shared.spawn_output_drainers(receivers);
        Ok(shared)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn input_sender(&self) -> SessionInputSender {
        self.input_sender.clone()
    }

    pub fn participant_handle(&self, participant: Participant) -> ParticipantSessionHandle {
        ParticipantSessionHandle::new(self.input_sender(), participant)
    }

    pub fn subscribe_outputs(&self) -> SharedPipelineOutputReceivers {
        SharedPipelineOutputReceivers::new(
            self.audio_tx.subscribe(),
            self.video_tx.subscribe(),
            self.data_tx.subscribe(),
        )
    }

    pub async fn is_active(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && self.session_handle.lock().await.is_active()
    }

    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.session_handle.lock().await.close().await
    }

    fn spawn_output_drainers(self: &Arc<Self>, receivers: ClientOutputReceivers) {
        let ClientOutputReceivers {
            audio_rx,
            video_rx,
            data_rx,
        } = receivers;

        Self::spawn_output_drainer(audio_rx, self.audio_tx.clone());
        Self::spawn_output_drainer(video_rx, self.video_tx.clone());
        Self::spawn_output_drainer(data_rx, self.data_tx.clone());
    }

    fn spawn_output_drainer(
        mut rx: mpsc::Receiver<RuntimeData>,
        tx: broadcast::Sender<TransportData>,
    ) {
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                let _ = tx.send(TransportData::new(data));
            }
        });
    }
}
