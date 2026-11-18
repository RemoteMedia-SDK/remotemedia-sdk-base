//! `StreamingPipelineService.StreamPipeline` on the control address
//! (openspec: extend-ai-sdk-media-streaming, design D10).
//!
//! A live pipeline session for a client of the control address (the AI SDK
//! provider's `remote()` adapter): the first message is `StreamInit` with the
//! manifest as `manifest_json`; every `DataChunk` after it is one input value,
//! which the session delivers to the pipeline's sources (`node_id` is not
//! used); every output comes back as a `ChunkResult` under the key `out`.
//! The stream ending, or `StreamControl` close/cancel, closes the session.
//!
//! It runs on a core [`PipelineExecutor`] session, as `RunPipeline` does, so
//! the graph behaves as it does in-process and over WebRTC (multi-output
//! nodes such as a VAD that passes audio and emits events). The data-plane
//! `StreamingServiceImpl` uses the older gRPC session router, which does not.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::PipelineExecutor;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::generated::stream_request::Request as StreamRequestKind;
use crate::generated::stream_response::Response as StreamResponseKind;
use crate::generated::streaming_pipeline_service_server::StreamingPipelineService;
use crate::generated::{ChunkResult, StreamClosed, StreamReady, StreamRequest, StreamResponse};

/// Key of every output in a [`ChunkResult`].
pub const LIVE_OUTPUT_KEY: &str = "out";

/// See the module documentation.
pub struct ControlStreamService {
    executor: Arc<PipelineExecutor>,
}

impl ControlStreamService {
    pub fn new(executor: Arc<PipelineExecutor>) -> Self {
        Self { executor }
    }
}

fn manifest_of(request: &StreamRequest) -> Result<Manifest, Status> {
    let Some(StreamRequestKind::Init(init)) = &request.request else {
        return Err(Status::invalid_argument("the first message must be StreamInit"));
    };
    let manifest = init
        .manifest
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("StreamInit has no manifest"))?;
    if manifest.manifest_json.is_empty() {
        return Err(Status::invalid_argument(
            "StreamInit.manifest needs manifest_json on the control address",
        ));
    }
    serde_json::from_slice(&manifest.manifest_json)
        .map_err(|e| Status::invalid_argument(format!("invalid manifest: {e}")))
}

fn result(buffer: crate::generated::DataBuffer, sequence: u64) -> StreamResponse {
    StreamResponse {
        response: Some(StreamResponseKind::Result(ChunkResult {
            sequence,
            data_outputs: [(LIVE_OUTPUT_KEY.to_string(), buffer)].into_iter().collect(),
            processing_time_ms: 0.0,
            total_items_processed: 0,
        })),
    }
}

#[tonic::async_trait]
impl StreamingPipelineService for ControlStreamService {
    type StreamPipelineStream = Pin<Box<dyn Stream<Item = Result<StreamResponse, Status>> + Send>>;

    async fn stream_pipeline(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Self::StreamPipelineStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the stream ended before StreamInit"))?;
        let manifest = manifest_of(&first)?;
        let mut handle = self
            .executor
            .create_session(Arc::new(manifest))
            .await
            .map_err(|e| Status::failed_precondition(format!("the pipeline did not start: {e}")))?;
        let input = handle
            .input_sender()
            .ok_or_else(|| Status::internal("the session has no input"))?;
        let session_id = input.session_id().to_string();

        let (tx, rx) = mpsc::channel::<Result<StreamResponse, Status>>(64);
        let _ = tx
            .send(Ok(StreamResponse {
                response: Some(StreamResponseKind::Ready(StreamReady {
                    session_id: session_id.clone(),
                    recommended_chunk_size: 0,
                    max_buffer_latency_ms: 0,
                })),
            }))
            .await;

        tokio::spawn(async move {
            // Inputs from the client, on a task of their own: the session's
            // outputs are drained while it is being fed.
            let (ended_tx, mut ended_rx) = tokio::sync::oneshot::channel::<Option<String>>();
            let feeder = tokio::spawn(async move {
                let reason = loop {
                    match inbound.message().await {
                        Ok(Some(StreamRequest { request: Some(StreamRequestKind::DataChunk(chunk)) })) => {
                            let Some(buffer) = chunk.buffer.or_else(|| chunk.named_buffers.into_values().next()) else {
                                continue;
                            };
                            let Some(data) = crate::adapters::data_buffer_to_transport_data(&buffer) else {
                                break Some(format!("input {} is not a value the runtime reads", chunk.sequence));
                            };
                            if let Err(e) = input.send(data).await {
                                break Some(format!("feeding the pipeline failed: {e}"));
                            }
                        }
                        Ok(Some(StreamRequest { request: Some(StreamRequestKind::Control(_)) })) => break None,
                        Ok(Some(_)) => continue,
                        Ok(None) => break None,
                        Err(status) => break Some(format!("the client stream failed: {status}")),
                    }
                };
                let _ = ended_tx.send(reason);
            });
            let mut sequence = 0u64;
            let reason = loop {
                tokio::select! {
                    _ = tx.closed() => break "the client went away".to_string(),
                    ended = &mut ended_rx => {
                        if let Ok(Some(message)) = ended {
                            let _ = tx.send(Err(Status::internal(message.clone()))).await;
                            break message;
                        }
                        break "the client closed the session".to_string();
                    }
                    output = handle.recv_output() => match output {
                        Ok(Some(data)) => {
                            let buffer = crate::adapters::transport_data_to_data_buffer(&data);
                            if tx.send(Ok(result(buffer, sequence))).await.is_err() {
                                break "the client went away".to_string();
                            }
                            sequence += 1;
                        }
                        Ok(None) => break "the pipeline ended".to_string(),
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(format!("the pipeline failed: {e}")))).await;
                            break format!("the pipeline failed: {e}");
                        }
                    },
                }
            };
            feeder.abort();
            if let Err(e) = handle.close().await {
                tracing::debug!("closing a live pipeline session: {e}");
            }
            let _ = tx
                .send(Ok(StreamResponse {
                    response: Some(StreamResponseKind::Closed(StreamClosed {
                        session_id,
                        final_metrics: None,
                        reason,
                    })),
                }))
                .await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
