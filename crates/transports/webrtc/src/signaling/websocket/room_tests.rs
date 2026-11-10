//! Two browser-like peers in one WebSocket room (openspec:
//! add-multi-participant-sessions, multi-participant-ingress): both join one
//! shared session, every data-channel output carries the `participant.*`
//! metadata of the peer whose input produced it, and a peer that hangs up
//! leaves the session.
use super::*;
use crate::generated::{data_buffer::DataType, DataBuffer};
use prost::Message as _;
use remotemedia_core::manifest::NodeManifest;
use remotemedia_core::transport::{data::participant, ExecutorConfig};
use remotemedia_types::{CopyContext, CopyPolicy, RuntimeData};
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::RTCPeerConnection;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Next JSON-RPC response with `id` (notifications are skipped).
async fn response(ws: &mut Ws, id: u64) -> Value {
    loop {
        let message = timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("signaling reply in time")
            .expect("socket open")
            .unwrap();
        if let Message::Text(text) = message {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == json!(id) {
                return value;
            }
        }
    }
}

async fn request(ws: &mut Ws, method: &str, params: Value, id: u64) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc": "2.0", "method": method, "params": params, "id": id}).to_string(),
    ))
    .await
    .unwrap();
    response(ws, id).await
}

struct Client {
    rtc: Arc<RTCPeerConnection>,
    channel: Arc<RTCDataChannel>,
    /// `(participant.id, text)` of every data-channel output received.
    outputs: mpsc::UnboundedReceiver<(Option<String>, String)>,
}

/// Announce, offer a data channel to the server, apply the answer and wait
/// for the channel to open.
async fn join(addr: std::net::SocketAddr, peer_id: &str) -> Client {
    let (mut ws, _) = connect_async(format!("ws://{addr}")).await.unwrap();
    let announced = request(
        &mut ws,
        "peer.announce",
        json!({"peer_id": peer_id, "capabilities": ["data"]}),
        1,
    )
    .await;
    assert!(announced.get("error").is_none(), "{announced}");

    // Host candidates on the machine's interfaces: the server peer gathers
    // no loopback candidates, so a loopback-only client could not pair.
    let mut settings = webrtc::api::setting_engine::SettingEngine::default();
    settings.set_network_types(vec![webrtc::ice::network_type::NetworkType::Udp4]);
    let rtc = Arc::new(
        webrtc::api::APIBuilder::new()
            .with_setting_engine(settings)
            .build()
            .new_peer_connection(Default::default())
            .await
            .unwrap(),
    );
    let channel = rtc.create_data_channel("data", None).await.unwrap();
    let (opened_tx, mut opened_rx) = mpsc::channel::<()>(1);
    channel.on_open(Box::new(move || {
        let opened_tx = opened_tx.clone();
        Box::pin(async move {
            let _ = opened_tx.send(()).await;
        })
    }));
    let (out_tx, outputs) = mpsc::unbounded_channel();
    channel.on_message(Box::new(move |message| {
        let out_tx = out_tx.clone();
        Box::pin(async move {
            let buffer = DataBuffer::decode(message.data).expect("DataBuffer output");
            let Some(DataType::Text(text)) = buffer.data_type else { return };
            let text = String::from_utf8(text.text_data).unwrap();
            let _ = out_tx.send((buffer.metadata.get(participant::ID).cloned(), text));
        })
    }));

    let mut gathered = rtc.gathering_complete_promise().await;
    rtc.set_local_description(rtc.create_offer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    let offer = rtc.local_description().await.unwrap().sdp;
    let answer = request(
        &mut ws,
        "peer.offer",
        json!({"from": peer_id, "to": "remotemedia-server", "sdp": offer}),
        2,
    )
    .await;
    let sdp = answer["result"]["sdp"].as_str().unwrap_or_else(|| panic!("{answer}"));
    rtc.set_remote_description(
        webrtc::peer_connection::sdp::session_description::RTCSessionDescription::answer(sdp.into())
            .unwrap(),
    )
    .await
    .unwrap();
    // Apply the server's trickled candidates for the life of the client.
    let trickle = Arc::clone(&rtc);
    tokio::spawn(async move {
        while let Some(Ok(message)) = ws.next().await {
            let Message::Text(text) = message else { continue };
            let Ok(value) = serde_json::from_str::<Value>(&text) else { continue };
            if value["method"] != "peer.ice_candidate" {
                continue;
            }
            let params = &value["params"];
            let candidate = params["candidate"].as_str().unwrap_or_default();
            if candidate.is_empty() {
                continue;
            }
            let _ = trickle
                .add_ice_candidate(webrtc::ice_transport::ice_candidate::RTCIceCandidateInit {
                    candidate: candidate.to_string(),
                    sdp_mid: params["sdp_mid"].as_str().map(str::to_string),
                    sdp_mline_index: params["sdp_m_line_index"].as_u64().map(|i| i as u16),
                    username_fragment: None,
                })
                .await;
        }
    });
    timeout(Duration::from_secs(20), opened_rx.recv())
        .await
        .expect("data channel opens")
        .unwrap();
    Client { rtc, channel, outputs }
}

impl Client {
    async fn say(&self, text: &str) {
        let wire = crate::adapters::encode_runtime_data_with_context(
            &RuntimeData::Text(text.into()),
            &CopyContext::new(CopyPolicy::AllowCopies, "room-test"),
        )
        .unwrap();
        self.channel.send(&wire).await.unwrap();
    }

    async fn next(&mut self) -> (Option<String>, String) {
        timeout(Duration::from_secs(10), self.outputs.recv())
            .await
            .expect("output in time")
            .expect("channel open")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peers_in_a_room_share_one_session_with_attributed_outputs() {
    let manifest = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "echo".into(),
            node_type: "PassThrough".into(),
            params: json!({}),
            // One instance per peer, so a peer's departure is observable.
            instance_scope: Some(remotemedia_core::nodes::InstanceScope::PerParticipant),
            ..Default::default()
        }],
        ..Default::default()
    });
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
        shared_session_key: Some("standup".into()),
        ..Default::default()
    });
    let state = Arc::new(SharedState::new(config, executor.clone(), manifest));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            let state = Arc::clone(&server_state);
            tokio::spawn(async move {
                let _ = handle_connection(stream, state).await;
            });
        }
    });

    let mut alice = join(addr, "alice").await;
    let mut bob = join(addr, "bob").await;

    // One session for the room.
    let room = state.rooms.lock().await.get("standup").cloned().expect("room session");
    let room_session = room.session_id().to_string();
    for peer in ["alice", "bob"] {
        let server_peer = state.server_peers.read().await.get(peer).cloned().unwrap();
        assert_eq!(server_peer.session_id().await.as_deref(), Some(room_session.as_str()));
    }

    alice.say("hi from alice").await;
    // Every attached peer receives every output (v1), each attributed.
    assert_eq!(alice.next().await, (Some("alice".into()), "hi from alice".into()));
    assert_eq!(bob.next().await, (Some("alice".into()), "hi from alice".into()));
    bob.say("hello from bob").await;
    assert_eq!(alice.next().await, (Some("bob".into()), "hello from bob".into()));
    assert_eq!(bob.next().await, (Some("bob".into()), "hello from bob".into()));

    let telemetry = executor
        .control_bus()
        .get(&room_session)
        .and_then(|control| control.telemetry())
        .expect("room telemetry");
    assert_eq!(telemetry.live_instances("echo"), 2, "an instance per peer");

    // Alice hangs up: she leaves the session and her instance stops; the
    // room keeps serving Bob.
    alice.rtc.close().await.unwrap();
    let server_alice = state.server_peers.read().await.get("alice").cloned().unwrap();
    server_alice.shutdown().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while telemetry.live_instances("echo") != 1 {
        assert!(std::time::Instant::now() < deadline, "alice did not leave");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bob.say("still here").await;
    assert_eq!(bob.next().await, (Some("bob".into()), "still here".into()));
    assert!(room.is_active().await);
    bob.rtc.close().await.unwrap();
}
