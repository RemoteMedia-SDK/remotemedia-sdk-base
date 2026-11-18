//! `RuntimeControl` over a real gRPC connection (openspec
//! `add-ai-sdk-voice-library`, task 6.2): the service next to
//! `PipelineControl` on one listener, the bearer token, the `UNIMPLEMENTED`
//! answers that name the operation, member events, and a one-shot pipeline
//! run on a real `PipelineExecutor`.

// `RunPipeline` is deprecated (it feeds pipeline input through the control
// address); its behaviour is still covered here while it exists.
#![allow(deprecated)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::adapters::{data_buffer_to_runtime_data, runtime_data_to_data_buffer};
use remotemedia_grpc::control_host::{ControlHost, MemberLifecycle, RunOutputs};
use remotemedia_grpc::generated::member_event::Event as MemberEventKind;
use remotemedia_grpc::generated::pipeline_control_client::PipelineControlClient;
use remotemedia_grpc::generated::runtime_control_client::RuntimeControlClient;
use remotemedia_grpc::generated::{
    AdmitMemberRequest, AdmitMemberResponse, DataBuffer, EndSessionRequest, GetSessionRequest,
    ListMembersRequest, ListSessionsRequest, ManagedLlmRequest, MemberChange, MemberInfo,
    RegisterPipelineRequest, RemoveMemberRequest, RunPipelineRequest, WatchMembersRequest,
};
use tokio::sync::broadcast;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

/// A host that does nothing: every call falls through to the trait default.
struct BareHost;
impl ControlHost for BareHost {}

/// A host with admission (canned), member events and real pipeline runs.
struct TestHost {
    executor: Arc<PipelineExecutor>,
    members: broadcast::Sender<MemberLifecycle>,
}

#[tonic::async_trait]
impl ControlHost for TestHost {
    async fn admit_member(
        &self,
        request: AdmitMemberRequest,
    ) -> Result<AdmitMemberResponse, Status> {
        if request.pipeline != "voice" {
            return Err(Status::not_found(format!(
                "pipeline '{}' is not served by this runtime",
                request.pipeline
            )));
        }
        Ok(AdmitMemberResponse {
            ticket: format!("ticket-for-{}", request.member.unwrap_or_default().id),
            signal_url: "ws://127.0.0.1:1/ws".into(),
            expires_at_ms: 1_000 * u64::from(request.ttl_secs),
            session_id: format!("session-{}", request.session_key),
        })
    }

    async fn list_members(&self, _session_id: &str) -> Result<Vec<MemberInfo>, Status> {
        Ok(vec![member("ada")])
    }

    async fn remove_member(&self, _session_id: &str, member_id: &str) -> Result<bool, Status> {
        Ok(member_id == "ada")
    }

    fn member_events(&self) -> Result<broadcast::Receiver<MemberLifecycle>, Status> {
        Ok(self.members.subscribe())
    }

    async fn run_pipeline(
        &self,
        manifest_json: String,
        inputs: Vec<DataBuffer>,
    ) -> Result<RunOutputs, Status> {
        remotemedia_grpc::control_host::run_pipeline(self.executor.clone(), &manifest_json, inputs)
            .await
    }

    async fn register_pipeline(&self, name: &str, manifest_json: &str) -> Result<bool, Status> {
        if manifest_json.trim().is_empty() {
            return Err(Status::invalid_argument("no manifest"));
        }
        Ok(name != "voice")
    }
}

fn member(id: &str) -> MemberInfo {
    MemberInfo {
        id: id.into(),
        name: "Ada".into(),
        metadata: HashMap::from([("plan".to_string(), "pro".to_string())]),
    }
}

struct Server {
    channel: Channel,
    executor: Arc<PipelineExecutor>,
    members: broadcast::Sender<MemberLifecycle>,
}

/// Serve `PipelineControl` + `RuntimeControl` on a free port. `bare`: the
/// host implements nothing.
async fn serve(token: Option<&str>, bare: bool) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let (members, _) = broadcast::channel(16);
    let host: Arc<dyn ControlHost> = if bare {
        Arc::new(BareHost)
    } else {
        Arc::new(TestHost {
            executor: executor.clone(),
            members: members.clone(),
        })
    };
    let bus = executor.control_bus();
    let token = token.map(str::to_string);
    tokio::spawn(async move {
        remotemedia_grpc::serve_control_with_host_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            bus,
            token,
            host,
        )
        .await
        .unwrap();
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Server {
        channel,
        executor,
        members,
    }
}

fn with_token<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn admit(pipeline: &str) -> AdmitMemberRequest {
    AdmitMemberRequest {
        pipeline: pipeline.into(),
        session_key: "room-1".into(),
        member: Some(member("ada")),
        ttl_secs: 60,
        grants: vec![],
        end_on_leave: false,
    }
}

#[tokio::test]
async fn a_host_that_does_nothing_answers_unimplemented_naming_each_operation() {
    let server = serve(None, true).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());

    let cases: Vec<(&str, Status)> = vec![
        (
            "registering a pipeline",
            client
                .register_pipeline(RegisterPipelineRequest {
                    name: "p".into(),
                    manifest_json: "{}".into(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "admitting a member",
            client.admit_member(admit("voice")).await.unwrap_err(),
        ),
        (
            "sessions by application key",
            client
                .get_session(GetSessionRequest {
                    session_id: "s".into(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "listing members",
            client
                .list_members(ListMembersRequest {
                    session_id: "s".into(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "removing a member",
            client
                .remove_member(RemoveMemberRequest {
                    session_id: "s".into(),
                    member_id: "m".into(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "ending a session",
            client
                .end_session(EndSessionRequest {
                    session_id: "s".into(),
                    reason: String::new(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "member events",
            client
                .watch_members(WatchMembersRequest {})
                .await
                .unwrap_err(),
        ),
        (
            "one-shot pipeline runs",
            client
                .run_pipeline(RunPipelineRequest {
                    manifest_json: "{}".into(),
                    inputs: vec![],
                })
                .await
                .unwrap_err(),
        ),
        (
            "managed language models",
            client
                .managed_llm(ManagedLlmRequest { model: "m".into() })
                .await
                .unwrap_err(),
        ),
    ];
    for (operation, status) in cases {
        assert_eq!(status.code(), Code::Unimplemented, "{operation}");
        assert!(
            status.message().contains(operation),
            "{operation}: {}",
            status.message()
        );
    }

    // `PipelineControl` is served on the same listener. (Its session list is
    // the process-wide bus, which other tests here add sessions to, so only
    // the call's success is checked.)
    PipelineControlClient::new(server.channel.clone())
        .list_sessions(ListSessionsRequest {})
        .await
        .unwrap();
}

#[tokio::test]
async fn both_services_require_the_bearer_token() {
    let server = serve(Some("secret"), false).await;
    let mut runtime = RuntimeControlClient::new(server.channel.clone());
    let mut control = PipelineControlClient::new(server.channel.clone());

    let refused = runtime.admit_member(admit("voice")).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
    let refused = runtime
        .admit_member(with_token(admit("voice"), "wrong"))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
    let refused = runtime
        .run_pipeline(RunPipelineRequest {
            manifest_json: "{}".into(),
            inputs: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
    let refused = control
        .list_sessions(ListSessionsRequest {})
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);

    let admitted = runtime
        .admit_member(with_token(admit("voice"), "secret"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(admitted.ticket, "ticket-for-ada");
    assert_eq!(admitted.session_id, "session-room-1");
    assert_eq!(admitted.expires_at_ms, 60_000);
    control
        .list_sessions(with_token(ListSessionsRequest {}, "secret"))
        .await
        .unwrap();
}

#[tokio::test]
async fn host_statuses_and_results_reach_the_client_as_they_are() {
    let server = serve(None, false).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());

    let unknown = client.admit_member(admit("other")).await.unwrap_err();
    assert_eq!(unknown.code(), Code::NotFound);
    assert!(unknown.message().contains("pipeline 'other'"));

    let members = client
        .list_members(ListMembersRequest {
            session_id: "s".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .members;
    assert_eq!(members, vec![member("ada")]);

    let remove = |member_id: &str| RemoveMemberRequest {
        session_id: "s".into(),
        member_id: member_id.into(),
    };
    assert!(
        client
            .remove_member(remove("ada"))
            .await
            .unwrap()
            .into_inner()
            .removed
    );
    assert!(
        !client
            .remove_member(remove("bob"))
            .await
            .unwrap()
            .into_inner()
            .removed
    );

    // What this host did not implement is still refused by name.
    let unimplemented = client
        .managed_llm(ManagedLlmRequest { model: "m".into() })
        .await
        .unwrap_err();
    assert_eq!(unimplemented.code(), Code::Unimplemented);
    assert!(unimplemented.message().contains("managed language models"));
}

#[tokio::test]
async fn watch_members_starts_with_ready_then_streams_joins_and_leaves() {
    let server = serve(None, false).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());
    let mut stream = client
        .watch_members(WatchMembersRequest {})
        .await
        .unwrap()
        .into_inner();
    let first = stream.message().await.unwrap().unwrap();
    assert!(matches!(first.event, Some(MemberEventKind::Ready(_))));

    let change = |reason: &str| MemberChange {
        session_id: "session-1".into(),
        member: Some(member("ada")),
        reason: reason.into(),
    };
    server
        .members
        .send(MemberLifecycle::Joined(change("")))
        .unwrap();
    server
        .members
        .send(MemberLifecycle::Left(change("removed")))
        .unwrap();
    match stream.message().await.unwrap().unwrap().event {
        Some(MemberEventKind::Joined(joined)) => {
            assert_eq!(joined.session_id, "session-1");
            assert_eq!(joined.member, Some(member("ada")));
        }
        other => panic!("expected joined, got {other:?}"),
    }
    match stream.message().await.unwrap().unwrap().event {
        Some(MemberEventKind::Left(left)) => assert_eq!(left.reason, "removed"),
        other => panic!("expected left, got {other:?}"),
    }
}

#[tokio::test]
async fn run_pipeline_streams_every_output_then_ends_and_leaves_no_session() {
    let server = serve(None, false).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());
    // Every executor in this process shares one session bus, and other tests
    // here open sessions of their own; a unique pipeline name lets this test
    // wait for its own session only.
    const PIPELINE: &str = "run-pipeline-one-shot";
    let manifest = serde_json::json!({
        "version": "v1",
        "metadata": { "name": PIPELINE },
        "nodes": [{ "id": "pass", "node_type": "PassThrough", "params": {} }],
        "connections": []
    });
    let inputs = vec![
        runtime_data_to_data_buffer(&RuntimeData::Text("hello".into())),
        runtime_data_to_data_buffer(&RuntimeData::Json(serde_json::json!({ "n": 1 }))),
        runtime_data_to_data_buffer(&RuntimeData::Text("bye".into())),
    ];
    let mut stream = client
        .run_pipeline(RunPipelineRequest {
            manifest_json: manifest.to_string(),
            inputs,
        })
        .await
        .unwrap()
        .into_inner();
    let mut outputs = Vec::new();
    while let Some(output) = tokio::time::timeout(Duration::from_secs(20), stream.message())
        .await
        .expect("the run ends")
        .unwrap()
    {
        outputs.push(data_buffer_to_runtime_data(&output.data.unwrap()).unwrap());
    }
    let texts: Vec<String> = outputs
        .iter()
        .map(|data| match data {
            RuntimeData::Text(text) => text.clone(),
            RuntimeData::Json(value) => value.to_string(),
            other => panic!("unexpected output {other:?}"),
        })
        .collect();
    assert_eq!(texts, vec!["hello", r#"{"n":1}"#, "bye"]);

    // The run's session is gone once the stream has ended.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let listed = || {
        server
            .executor
            .control_bus()
            .list()
            .iter()
            .any(|session| session.pipeline_name == PIPELINE)
    };
    while listed() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the one-shot session is still listed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn run_pipeline_refuses_a_manifest_it_cannot_read_or_start() {
    let server = serve(None, false).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());
    let invalid = client
        .run_pipeline(RunPipelineRequest {
            manifest_json: "not json".into(),
            inputs: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);

    let manifest = serde_json::json!({
        "version": "v1",
        "metadata": { "name": "bad" },
        "nodes": [{ "id": "x", "node_type": "NoSuchNodeType", "params": {} }],
        "connections": []
    });
    let unknown = client
        .run_pipeline(RunPipelineRequest {
            manifest_json: manifest.to_string(),
            inputs: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::FailedPrecondition);
    assert!(
        unknown.message().contains("did not start"),
        "{}",
        unknown.message()
    );
}

#[tokio::test]
async fn a_control_server_without_a_host_has_no_runtime_control_service() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let bus = executor.control_bus();
    tokio::spawn(async move {
        remotemedia_grpc::serve_control_with_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            bus,
            None,
        )
        .await
        .unwrap();
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let refused = RuntimeControlClient::new(channel)
        .admit_member(admit("voice"))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
}

#[tokio::test]
async fn register_pipeline_reaches_the_host_and_needs_a_name() {
    let server = serve(None, false).await;
    let mut client = RuntimeControlClient::new(server.channel.clone());
    let added = client
        .register_pipeline(RegisterPipelineRequest {
            name: "provider-0123456789abcdef".into(),
            manifest_json: "{\"version\":\"v1\",\"nodes\":[]}".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(added.registered);
    // The same name again: already there.
    let again = client
        .register_pipeline(RegisterPipelineRequest {
            name: "voice".into(),
            manifest_json: "{}".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!again.registered);
    let refused = client
        .register_pipeline(RegisterPipelineRequest {
            name: "  ".into(),
            manifest_json: "{}".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert!(refused.message().contains("name"), "{}", refused.message());
}

#[tokio::test]
async fn the_control_address_serves_no_pipeline_input() {
    // Pipeline input crosses the pipeline's own transport (design D10):
    // `StreamPipeline` is not on the control address.
    use remotemedia_grpc::generated::streaming_pipeline_service_client::StreamingPipelineServiceClient;
    let server = serve(None, false).await;
    let mut client = StreamingPipelineServiceClient::new(server.channel.clone());
    let refused = client
        .stream_pipeline(tokio_stream::iter(Vec::new()))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unimplemented);
}
