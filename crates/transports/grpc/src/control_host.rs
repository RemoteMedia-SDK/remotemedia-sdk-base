//! The gRPC `RuntimeControl` service (openspec `add-ai-sdk-voice-library`,
//! task 6.2): what an application's backend needs from a runtime it does not
//! embed. `AdmitMember`, `GetSession`, `ListMembers`, `RemoveMember`,
//! `EndSession`, `WatchMembers`, `RunPipeline` and `ManagedLlm`
//! (`proto/runtime_control.proto`).
//!
//! The service knows nothing about signalling servers, tickets or model
//! servers. The process hosting the runtime implements [`ControlHost`] for
//! what it has and serves it next to `PipelineControl`
//! ([`serve_control_with_host`](crate::control::serve_control_with_host)).
//! Every trait method has a default that answers `UNIMPLEMENTED` with a
//! message naming the operation, so a client can tell exactly what a given
//! runtime does not do.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{PipelineExecutor, TransportData};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::generated::member_event::Event as PbMemberEvent;
use crate::generated::runtime_control_server::RuntimeControl;
use crate::generated::{
    AdmitMemberRequest, AdmitMemberResponse, DataBuffer, EndSessionRequest, EndSessionResponse,
    GetSessionRequest, GetSessionResponse, ListMembersRequest, ListMembersResponse,
    ManagedLlmRequest, ManagedLlmResponse, MemberChange, MemberEvent, MemberInfo,
    RemoveMemberRequest, RemoveMemberResponse, RunPipelineOutput, RunPipelineRequest,
    WatchMembersReady, WatchMembersRequest,
};

/// A member joined or left a session the host admits members to.
#[derive(Debug, Clone)]
pub enum MemberLifecycle {
    Joined(MemberChange),
    Left(MemberChange),
}

/// `UNIMPLEMENTED`, naming the operation this runtime does not do.
pub fn unimplemented(operation: &str) -> Status {
    Status::unimplemented(format!("this runtime does not support {operation}"))
}

/// Outputs of a pipeline run, in the order they were produced. The run ends
/// when the channel closes; dropping the receiver cancels it.
pub type RunOutputs = mpsc::Receiver<Result<DataBuffer, Status>>;

/// The host side of `RuntimeControl`. Errors are gRPC statuses and reach the
/// client as they are.
#[tonic::async_trait]
pub trait ControlHost: Send + Sync + 'static {
    /// Start the session of `(pipeline, session_key)` if it is not running
    /// and mint a single-use ticket for the member.
    async fn admit_member(
        &self,
        _request: AdmitMemberRequest,
    ) -> Result<AdmitMemberResponse, Status> {
        Err(unimplemented("admitting a member"))
    }

    /// The application's key of a session started by an admission.
    /// `NOT_FOUND` for any other session. May wait for a session start in
    /// progress.
    async fn get_session(&self, _session_id: &str) -> Result<GetSessionResponse, Status> {
        Err(unimplemented("sessions by application key"))
    }

    /// The connected members of a session. `NOT_FOUND` for a session the
    /// host does not admit members to.
    async fn list_members(&self, _session_id: &str) -> Result<Vec<MemberInfo>, Status> {
        Err(unimplemented("listing members"))
    }

    /// Disconnect a member. `false` when it was not connected.
    async fn remove_member(&self, _session_id: &str, _member_id: &str) -> Result<bool, Status> {
        Err(unimplemented("removing a member"))
    }

    /// End a session. `false` when it is not live.
    async fn end_session(&self, _session_id: &str, _reason: &str) -> Result<bool, Status> {
        Err(unimplemented("ending a session"))
    }

    /// Members joining and leaving, from now on.
    fn member_events(&self) -> Result<broadcast::Receiver<MemberLifecycle>, Status> {
        Err(unimplemented("member events"))
    }

    /// Run a manifest on `inputs` in a session of its own
    /// ([`run_pipeline`] does it on an executor).
    async fn run_pipeline(
        &self,
        _manifest_json: String,
        _inputs: Vec<DataBuffer>,
    ) -> Result<RunOutputs, Status> {
        Err(unimplemented("one-shot pipeline runs"))
    }

    /// The managed OpenAI-compatible server for `model`, started on demand.
    /// `NOT_FOUND` for a model the host has no preset for.
    async fn managed_llm(&self, _model: &str) -> Result<ManagedLlmResponse, Status> {
        Err(unimplemented("managed language models"))
    }

    /// The executor that live pipeline sessions run on. With one, the control
    /// server also serves `StreamingPipelineService.StreamPipeline` on the
    /// same address and behind the same token, so a client of the control
    /// address can hold a session open for live input (openspec:
    /// extend-ai-sdk-media-streaming, design D10). `None` (the default):
    /// `StreamPipeline` answers `UNIMPLEMENTED` there.
    fn pipeline_executor(&self) -> Option<Arc<PipelineExecutor>> {
        None
    }
}

/// Run `manifest_json` on `inputs` in a new session of `executor` and stream
/// every output until the pipeline has drained. Dropping the receiver closes
/// the session.
pub async fn run_pipeline(
    executor: Arc<PipelineExecutor>,
    manifest_json: &str,
    inputs: Vec<DataBuffer>,
) -> Result<RunOutputs, Status> {
    let manifest: Manifest = serde_json::from_str(manifest_json)
        .map_err(|e| Status::invalid_argument(format!("invalid manifest: {e}")))?;
    let mut decoded: Vec<TransportData> = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.iter().enumerate() {
        decoded.push(
            crate::adapters::data_buffer_to_transport_data(input).ok_or_else(|| {
                Status::invalid_argument(format!("input {index} is not a value the runtime reads"))
            })?,
        );
    }
    let mut handle = executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(|e| Status::failed_precondition(format!("the pipeline did not start: {e}")))?;
    let sender = handle
        .input_sender()
        .ok_or_else(|| Status::internal("the session has no input"))?;

    let (tx, rx) = mpsc::channel::<Result<DataBuffer, Status>>(64);
    tokio::spawn(async move {
        // Inputs are fed from a task of their own: a pipeline that answers
        // while it is still being fed must have its outputs drained.
        let (fed_tx, mut fed_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let feeder = tokio::spawn(async move {
            let mut result = Ok(());
            for input in decoded {
                if let Err(e) = sender.send(input).await {
                    result = Err(e.to_string());
                    break;
                }
            }
            drop(sender);
            let _ = fed_tx.send(result);
        });
        let mut feeding = true;
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                fed = &mut fed_rx, if feeding => {
                    feeding = false;
                    if let Ok(Err(message)) = fed {
                        let _ = tx
                            .send(Err(Status::internal(format!("feeding the pipeline failed: {message}"))))
                            .await;
                        break;
                    }
                    handle.signal_input_complete();
                }
                output = handle.recv_output() => match output {
                    Ok(Some(data)) => {
                        let buffer = crate::adapters::transport_data_to_data_buffer(&data);
                        if tx.send(Ok(buffer)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("the pipeline failed: {e}")))).await;
                        break;
                    }
                },
            }
        }
        feeder.abort();
        if let Err(e) = handle.close().await {
            tracing::debug!("closing a one-shot pipeline session: {e}");
        }
    });
    Ok(rx)
}

/// The largest request `RuntimeControl` reads: `RunPipeline` carries its
/// inputs (audio to transcribe, for example) in one message.
pub const MAX_RUNTIME_CONTROL_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// gRPC `RuntimeControl` over a [`ControlHost`].
#[derive(Clone)]
pub struct RuntimeControlService {
    host: Arc<dyn ControlHost>,
    /// Bearer token every request must carry, as on `PipelineControl`.
    token: Option<Arc<str>>,
}

impl RuntimeControlService {
    pub fn new(host: Arc<dyn ControlHost>) -> Self {
        Self { host, token: None }
    }

    /// Require `authorization: Bearer <token>` on every RPC.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        let token: String = token.into();
        self.token = (!token.is_empty()).then(|| Arc::from(token.as_str()));
        self
    }

    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let Some(expected) = &self.token else {
            return Ok(());
        };
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
            });
        match got {
            Some(t) if t == expected.as_ref() => Ok(()),
            _ => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
    }
}

type MemberStream = Pin<Box<dyn Stream<Item = Result<MemberEvent, Status>> + Send + 'static>>;
type RunOutputStream =
    Pin<Box<dyn Stream<Item = Result<RunPipelineOutput, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl RuntimeControl for RuntimeControlService {
    type WatchMembersStream = MemberStream;
    type RunPipelineStream = RunOutputStream;

    async fn admit_member(
        &self,
        request: Request<AdmitMemberRequest>,
    ) -> Result<Response<AdmitMemberResponse>, Status> {
        self.check_auth(&request)?;
        Ok(Response::new(
            self.host.admit_member(request.into_inner()).await?,
        ))
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        self.check_auth(&request)?;
        Ok(Response::new(
            self.host
                .get_session(&request.into_inner().session_id)
                .await?,
        ))
    }

    async fn list_members(
        &self,
        request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        self.check_auth(&request)?;
        let members = self
            .host
            .list_members(&request.into_inner().session_id)
            .await?;
        Ok(Response::new(ListMembersResponse { members }))
    }

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<RemoveMemberResponse>, Status> {
        self.check_auth(&request)?;
        let request = request.into_inner();
        let removed = self
            .host
            .remove_member(&request.session_id, &request.member_id)
            .await?;
        Ok(Response::new(RemoveMemberResponse { removed }))
    }

    async fn end_session(
        &self,
        request: Request<EndSessionRequest>,
    ) -> Result<Response<EndSessionResponse>, Status> {
        self.check_auth(&request)?;
        let request = request.into_inner();
        let ended = self
            .host
            .end_session(&request.session_id, &request.reason)
            .await?;
        Ok(Response::new(EndSessionResponse { ended }))
    }

    async fn watch_members(
        &self,
        request: Request<WatchMembersRequest>,
    ) -> Result<Response<Self::WatchMembersStream>, Status> {
        self.check_auth(&request)?;
        let mut events = self.host.member_events()?;
        let (tx, rx) = mpsc::channel::<Result<MemberEvent, Status>>(256);
        tokio::spawn(async move {
            let ready = MemberEvent {
                event: Some(PbMemberEvent::Ready(WatchMembersReady {})),
            };
            if tx.send(Ok(ready)).await.is_err() {
                return;
            }
            loop {
                use tokio::sync::broadcast::error::RecvError;
                let event = tokio::select! {
                    _ = tx.closed() => return,
                    event = events.recv() => event,
                };
                let out = match event {
                    Ok(MemberLifecycle::Joined(change)) => Ok(MemberEvent {
                        event: Some(PbMemberEvent::Joined(change)),
                    }),
                    Ok(MemberLifecycle::Left(change)) => Ok(MemberEvent {
                        event: Some(PbMemberEvent::Left(change)),
                    }),
                    // Events are not replayed. A watcher that fell behind is
                    // told so and opens a new stream (`ListMembers` gives the
                    // members of each session it knows).
                    Err(RecvError::Lagged(missed)) => Err(Status::data_loss(format!(
                        "the watcher fell behind and missed {missed} member events"
                    ))),
                    Err(RecvError::Closed) => return,
                };
                let failed = out.is_err();
                if tx.send(out).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn run_pipeline(
        &self,
        request: Request<RunPipelineRequest>,
    ) -> Result<Response<Self::RunPipelineStream>, Status> {
        self.check_auth(&request)?;
        let request = request.into_inner();
        let outputs = self
            .host
            .run_pipeline(request.manifest_json, request.inputs)
            .await?;
        let stream = ReceiverStream::new(outputs)
            .map(|output| output.map(|data| RunPipelineOutput { data: Some(data) }));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn managed_llm(
        &self,
        request: Request<ManagedLlmRequest>,
    ) -> Result<Response<ManagedLlmResponse>, Status> {
        self.check_auth(&request)?;
        Ok(Response::new(
            self.host.managed_llm(&request.into_inner().model).await?,
        ))
    }
}
