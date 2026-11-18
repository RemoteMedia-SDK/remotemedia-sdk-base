//! The member peer against a real signalling server with ticket admission
//! (openspec extend-ai-sdk-media-streaming, design D11–D12): a member joins
//! with a ticket, exchanges lossless frames (audio as data, chunked when
//! large), ends its input and sees the session drain, and an `end_on_leave`
//! session ends when it leaves.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use remotemedia_core::manifest::{Manifest, ManifestMetadata, NodeManifest};
use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor};
use remotemedia_types::{CopyPolicy, RuntimeData};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::agent_host::{AgentHost, AgentHostConfig, AgentMember, AgentTicketRequest};
use crate::config::WebRtcTransportConfig;
use crate::member::{MemberEvent, MemberPeer, MemberPeerConfig};
use crate::signaling::websocket::{handle_connection, SharedState};

struct Runtime {
    host: AgentHost,
}

fn echo_manifest() -> Arc<Manifest> {
    Arc::new(Manifest {
        metadata: ManifestMetadata {
            name: "echo".into(),
            ..Default::default()
        },
        nodes: vec![NodeManifest {
            id: "echo".into(),
            node_type: "PassThrough".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// A signalling server with ticket admission serving `echo`.
async fn runtime() -> Runtime {
    let manifest = echo_manifest();
    let executor = Arc::new(
        PipelineExecutor::with_config(ExecutorConfig {
            transport_materialization_policy: CopyPolicy::AllowCopies,
            ..Default::default()
        })
        .unwrap(),
    );
    let config = Arc::new(WebRtcTransportConfig {
        stun_servers: Vec::new(),
        turn_servers: Vec::new(),
        ..Default::default()
    });
    let (bridge_tx, bridge_rx) = tokio::sync::mpsc::channel(1024);
    let state = Arc::new(SharedState::new_with_events(
        config,
        executor,
        Arc::clone(&manifest),
        Some(bridge_tx),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let host = AgentHost::install(
        Arc::clone(&state),
        bridge_rx,
        AgentHostConfig {
            signal_url: format!("ws://{addr}"),
            empty_session_timeout: Duration::from_secs(600),
        },
    );
    host.register_pipeline("echo", manifest);
    let server_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let state = Arc::clone(&server_state);
            tokio::spawn(async move {
                let _ = handle_connection(stream, state).await;
            });
        }
    });
    Runtime { host }
}

impl Runtime {
    async fn admit(&self, key: &str, end_on_leave: bool) -> (MemberPeerConfig, String) {
        let ticket = self
            .host
            .mint_ticket(AgentTicketRequest {
                pipeline: "echo".into(),
                session_key: key.into(),
                member: AgentMember {
                    id: "caller".into(),
                    name: None,
                    metadata: HashMap::new(),
                },
                ttl: Duration::from_secs(60),
                grants: Vec::new(),
                end_on_leave,
            })
            .await
            .expect("ticket");
        (
            MemberPeerConfig::new(self.host.signal_url(), ticket.ticket),
            ticket.session_id,
        )
    }

    fn live(&self, session_id: &str) -> bool {
        self.host
            .sessions()
            .iter()
            .any(|session| session.session_id == session_id)
    }
}

async fn next_output(peer: &MemberPeer) -> RuntimeData {
    loop {
        match timeout(Duration::from_secs(15), peer.recv()).await {
            Ok(Some(MemberEvent::Output(data))) => return data,
            Ok(Some(other)) => panic!("expected an output, got {other:?}"),
            Ok(None) => panic!("member closed"),
            Err(_) => panic!("no output in time"),
        }
    }
}

fn tone(samples: usize) -> Vec<f32> {
    (0..samples)
        .map(|i| ((i as f32) * 0.01).sin() * 0.5)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_round_trips_audio_losslessly_and_drains() {
    let runtime = runtime().await;
    let (config, session_id) = runtime.admit("call-1", true).await;
    let peer = MemberPeer::connect(config).await.expect("member joins");

    // Small and large (chunked both ways: 200k f32 = 800 kB) audio.
    for samples in [tone(320), tone(200_000)] {
        peer.send(&RuntimeData::Audio {
            samples: samples.clone().into(),
            sample_rate: 16_000,
            channels: 1,
            stream_id: Some("mic".into()),
            timestamp_us: Some(1_000),
            arrival_ts_us: None,
            metadata: None,
        })
        .await
        .unwrap();
        match next_output(&peer).await {
            RuntimeData::Audio {
                samples: got,
                sample_rate,
                channels,
                stream_id,
                ..
            } => {
                assert_eq!(sample_rate, 16_000);
                assert_eq!(channels, 1);
                assert_eq!(stream_id.as_deref(), Some("mic"));
                assert_eq!(got.len(), samples.len());
                assert!(
                    got.iter().zip(&samples).all(|(a, b)| a == b),
                    "samples changed"
                );
            }
            other => panic!("expected audio, got {other:?}"),
        }
    }

    peer.send(&RuntimeData::Text("last".into())).await.unwrap();
    peer.end_input().await.unwrap();
    // Everything before the end, then the drain.
    assert!(matches!(next_output(&peer).await, RuntimeData::Text(t) if t == "last"));
    match timeout(Duration::from_secs(15), peer.recv()).await {
        Ok(Some(MemberEvent::Drained)) => {}
        other => panic!("expected the drain, got {other:?}"),
    }

    // `end_on_leave`: the session ends as soon as the member leaves.
    assert!(runtime.live(&session_id));
    peer.close().await;
    timeout(Duration::from_secs(10), async {
        while runtime.live(&session_id) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the session ends when its member leaves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_without_end_on_leave_outlives_its_member_and_ignores_end_of_input() {
    let runtime = runtime().await;
    let (config, session_id) = runtime.admit("room-1", false).await;
    let peer = MemberPeer::connect(config).await.expect("member joins");
    peer.end_input().await.unwrap();
    // Input still flows: the end was ignored.
    peer.send(&RuntimeData::Text("still here".into()))
        .await
        .unwrap();
    assert!(matches!(next_output(&peer).await, RuntimeData::Text(t) if t == "still here"));
    peer.close().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(runtime.live(&session_id), "kept until the empty timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_used_ticket_is_refused_with_the_servers_reason() {
    let runtime = runtime().await;
    let (config, _) = runtime.admit("call-2", true).await;
    let first = MemberPeer::connect(config.clone())
        .await
        .expect("first use");
    let error = match MemberPeer::connect(MemberPeerConfig {
        connect_timeout: Duration::from_secs(10),
        ..config
    })
    .await
    {
        Ok(_) => panic!("a used ticket joined"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.to_lowercase().contains("ticket") || error.to_lowercase().contains("admitted"),
        "{error}"
    );
    first.close().await;
}
