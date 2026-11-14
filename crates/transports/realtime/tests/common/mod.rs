//! Shared test fixtures: the signalling listener's connection handler on a
//! local port with a ticket admission, a WebSocket client, and a stub in
//! place of the session and its `RealtimeEventsNode`.
#![allow(dead_code)]

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use remotemedia_core::data::{AudioSamples, RuntimeData};
use remotemedia_core::manifest::{Manifest, NodeManifest};
use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor, TransportData};
use remotemedia_realtime::{MemberInput, MemberJoiner, MemberLink, Outbound, RealtimeConfig};
use remotemedia_webrtc::control::ControlAccess;
use remotemedia_webrtc::signaling::websocket::{
    handle_connection, AdmittedPeer, PeerAdmission, SharedState,
};
use remotemedia_webrtc::WebRtcTransportConfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const PATIENCE: Duration = Duration::from_secs(10);

// ── Admission ────────────────────────────────────────────────────────────

/// Single-use tickets, as a host's admission issues them.
pub struct Tickets(Mutex<HashMap<String, AdmittedPeer>>);

impl Tickets {
    pub fn new(tickets: &[(&str, AdmittedPeer)]) -> Arc<Self> {
        Arc::new(Self(Mutex::new(
            tickets
                .iter()
                .map(|(ticket, peer)| (ticket.to_string(), peer.clone()))
                .collect(),
        )))
    }
}

impl PeerAdmission for Tickets {
    fn admit(&self, ticket: Option<&str>) -> Result<AdmittedPeer, String> {
        let ticket = ticket.ok_or("no ticket")?;
        self.0
            .lock()
            .unwrap()
            .remove(ticket)
            .ok_or_else(|| "unknown or used ticket".to_string())
    }
}

pub fn member(id: &str) -> AdmittedPeer {
    AdmittedPeer {
        peer_id: id.to_string(),
        room: "room".to_string(),
        display_name: None,
        metadata: HashMap::new(),
        control: ControlAccess::Granted(Vec::new()),
    }
}

/// A member whose admission carries `rm.realtime` (a JSON string).
pub fn member_with(id: &str, realtime: Value) -> AdmittedPeer {
    let mut peer = member(id);
    peer.metadata
        .insert("rm.realtime".to_string(), Value::String(realtime.to_string()));
    peer
}

// ── Listener ─────────────────────────────────────────────────────────────

pub fn shared_state(manifest: Manifest, config: WebRtcTransportConfig) -> Arc<SharedState> {
    let executor = Arc::new(
        PipelineExecutor::with_config(ExecutorConfig {
            transport_materialization_policy: remotemedia_types::CopyPolicy::AllowCopies,
            ..Default::default()
        })
        .unwrap(),
    );
    Arc::new(SharedState::new(Arc::new(config), executor, Arc::new(manifest)))
}

pub fn passthrough_manifest() -> Manifest {
    Manifest {
        nodes: vec![NodeManifest {
            id: "echo".into(),
            node_type: "PassThrough".into(),
            params: json!({}),
            instance_scope: Some(remotemedia_core::nodes::InstanceScope::PerParticipant),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Serve the signalling listener's connection handler on a local port.
pub async fn listen(state: Arc<SharedState>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let _ = handle_connection(stream, state).await;
            });
        }
    });
    addr
}

// ── The stub session ─────────────────────────────────────────────────────

/// What the transport handed to the session for a member.
#[derive(Debug)]
pub enum Input {
    Audio { samples: Vec<f32>, sample_rate: u32 },
    Event(Value),
    Left,
}

struct StubInput(mpsc::UnboundedSender<Input>);

#[async_trait]
impl MemberInput for StubInput {
    async fn audio(&self, samples: Vec<f32>, sample_rate: u32) -> Result<(), String> {
        self.0
            .send(Input::Audio { samples, sample_rate })
            .map_err(|_| "gone".to_string())
    }
    async fn event(&self, frame: Value) -> Result<(), String> {
        self.0.send(Input::Event(frame)).map_err(|_| "gone".to_string())
    }
    async fn leave(&self) {
        let _ = self.0.send(Input::Left);
    }
}

#[derive(Default)]
struct Settings {
    lead_ms: Option<u64>,
}

struct StubJoiner {
    nodes: mpsc::UnboundedSender<Node>,
    settings: Arc<Mutex<Settings>>,
}

#[async_trait]
impl MemberJoiner for StubJoiner {
    async fn join(
        &self,
        _state: &Arc<SharedState>,
        member: &AdmittedPeer,
        _connection: &str,
    ) -> Result<MemberLink, String> {
        let (input_tx, inputs) = mpsc::unbounded_channel();
        let (outputs_tx, outputs) = mpsc::channel(1024);
        let _ = self.nodes.send(Node {
            member: member.peer_id.clone(),
            inputs,
            outputs: outputs_tx,
        });
        Ok(MemberLink {
            input: Arc::new(StubInput(input_tx)),
            outputs,
            lead_ms: self.settings.lock().unwrap().lead_ms,
            pipeline_sample_rate: None,
        })
    }
}

/// The session side of one connection: what a member's node and the
/// session's sinks would produce, and what they would receive.
pub struct Node {
    pub member: String,
    inputs: mpsc::UnboundedReceiver<Input>,
    outputs: mpsc::Sender<Outbound>,
}

impl Node {
    pub async fn send(&self, output: Outbound) {
        self.outputs.send(output).await.expect("connection open");
    }

    /// A frame the `participant`'s node instance wrote to its `realtime` port.
    pub async fn event(&self, participant: &str, frame: Value) {
        self.send(Outbound::Event {
            participant: Some(participant.to_string()),
            frame,
        })
        .await;
    }

    pub async fn audio_frame(
        &self,
        samples: Vec<f32>,
        rate: u32,
        metadata: &[(&str, &str)],
    ) {
        let mut frame = TransportData::new(RuntimeData::Audio {
            samples: AudioSamples::from(samples),
            sample_rate: rate,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        });
        for (key, value) in metadata {
            frame.metadata.insert(key.to_string(), value.to_string());
        }
        self.send(Outbound::Audio(frame)).await;
    }

    /// `ms` of TTS audio of `turn` produced for `participant`, with the
    /// metadata the router carries (`participant.id`, `participant.turn_id`)
    /// and, with `item`, the node's `rm.item_id`.
    pub async fn audio(&self, participant: &str, turn: &str, item: Option<&str>, rate: u32, ms: u64) {
        let mut extra = Vec::new();
        if let Some(item) = item {
            extra.push(("rm.item_id", item));
        }
        self.audio_with(participant, turn, rate, ms, &extra).await;
    }

    pub async fn audio_with(
        &self,
        participant: &str,
        turn: &str,
        rate: u32,
        ms: u64,
        extra: &[(&str, &str)],
    ) {
        let samples = vec![0.1f32; (u64::from(rate) * ms / 1000) as usize];
        self.audio_tagged(participant, turn, rate, samples, extra).await;
    }

    pub async fn audio_samples(&self, participant: &str, turn: &str, rate: u32, samples: Vec<f32>) {
        self.audio_tagged(participant, turn, rate, samples, &[]).await;
    }

    async fn audio_tagged(
        &self,
        participant: &str,
        turn: &str,
        rate: u32,
        samples: Vec<f32>,
        extra: &[(&str, &str)],
    ) {
        let mut metadata = vec![("participant.id", participant), ("participant.chunk", "0")];
        if !turn.is_empty() {
            metadata.push(("participant.turn_id", turn));
        }
        metadata.extend_from_slice(extra);
        self.audio_frame(samples, rate, &metadata).await;
    }

    pub async fn next_input(&mut self) -> Input {
        tokio::time::timeout(PATIENCE, self.inputs.recv())
            .await
            .expect("input from the transport in time")
            .expect("connection open")
    }

    /// The next frame for the node's `realtime` port (audio is skipped).
    pub async fn next_event(&mut self) -> Value {
        loop {
            match self.next_input().await {
                Input::Event(event) => return event,
                Input::Audio { .. } => {}
                Input::Left => panic!("the member left"),
            }
        }
    }

    pub fn try_event(&mut self) -> Option<Value> {
        loop {
            match self.inputs.try_recv().ok()? {
                Input::Event(event) => return Some(event),
                _ => {}
            }
        }
    }

    /// The next `released` report that satisfies `wanted`.
    pub async fn next_released(&mut self, wanted: impl Fn(&Value) -> bool) -> Value {
        loop {
            let event = self.next_event().await;
            if event["type"] == "remotemedia.internal.released" && wanted(&event) {
                return event;
            }
        }
    }

    /// Wait until the member has left the session.
    pub async fn left(&mut self) {
        loop {
            if matches!(self.next_input().await, Input::Left) {
                return;
            }
        }
    }
}

// ── Server ───────────────────────────────────────────────────────────────

pub struct Server {
    pub addr: SocketAddr,
    pub state: Arc<SharedState>,
    nodes: mpsc::UnboundedReceiver<Node>,
    settings: Arc<Mutex<Settings>>,
}

impl Server {
    /// The signalling listener with `/realtime` mounted over a stub session.
    pub async fn start(config: RealtimeConfig, tickets: &[(&str, AdmittedPeer)]) -> Self {
        let state = shared_state(passthrough_manifest(), WebRtcTransportConfig::default());
        state.set_admission(Tickets::new(tickets));
        let (nodes_tx, nodes) = mpsc::unbounded_channel();
        let settings = Arc::new(Mutex::new(Settings::default()));
        remotemedia_realtime::mount_with(
            &state,
            config,
            Arc::new(StubJoiner {
                nodes: nodes_tx,
                settings: Arc::clone(&settings),
            }),
        );
        let addr = listen(Arc::clone(&state)).await;
        Self {
            addr,
            state,
            nodes,
            settings,
        }
    }

    /// What the session's `RealtimeEventsNode` sets as `lead_ms`.
    pub fn set_lead_ms(&mut self, lead_ms: u64) {
        self.settings.lock().unwrap().lead_ms = Some(lead_ms);
    }

    /// Connect as the AI SDK model does: both subprotocols, no ticket in the URL.
    pub async fn connect(&self, ticket: &str) -> Result<Client, Option<u16>> {
        connect_url(
            &format!("ws://{}/realtime", self.addr),
            &["remotemedia.realtime.v1", &format!("remotemedia.ticket.{ticket}")],
        )
        .await
    }

    /// The session side of the next connection that joined.
    pub async fn node(&mut self) -> Node {
        tokio::time::timeout(PATIENCE, self.nodes.recv())
            .await
            .expect("a member joined in time")
            .expect("server running")
    }
}

// ── Client ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Client {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// The subprotocol the server answered.
    pub protocol: Option<String>,
}

/// Open a WebSocket offering `protocols`. `Err` carries the HTTP status of a
/// refused handshake.
pub async fn connect_url(url: &str, protocols: &[&str]) -> Result<Client, Option<u16>> {
    let mut request = url.into_client_request().unwrap();
    if !protocols.is_empty() {
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", protocols.join(", ").parse().unwrap());
    }
    match connect_async(request).await {
        Ok((socket, response)) => Ok(Client {
            socket,
            protocol: response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        }),
        Err(WsError::Http(response)) => Err(Some(response.status().as_u16())),
        Err(_) => Err(None),
    }
}

impl Client {
    pub async fn send(&mut self, frame: Value) {
        self.socket
            .send(Message::Text(frame.to_string()))
            .await
            .expect("socket open");
    }

    /// The next JSON frame.
    pub async fn next(&mut self) -> Value {
        self.next_before(Instant::now() + PATIENCE)
            .await
            .expect("a frame in time")
    }

    /// The next JSON frame, or `None` when none arrives before `deadline`.
    pub async fn next_before(&mut self, deadline: Instant) -> Option<Value> {
        loop {
            let message = tokio::time::timeout_at(deadline.into(), self.socket.next())
                .await
                .ok()??;
            match message {
                Ok(Message::Text(text)) => return Some(serde_json::from_str(&text).unwrap()),
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }

    /// Read until the server closes the socket; its close code.
    pub async fn closed(&mut self) -> Option<u16> {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            match tokio::time::timeout_at(deadline, self.socket.next()).await {
                Ok(Some(Ok(Message::Close(frame)))) => return frame.map(|frame| frame.code.into()),
                Ok(Some(Ok(_))) => {}
                _ => return None,
            }
        }
    }

    pub async fn close(&mut self) {
        let _ = self.socket.close(None).await;
    }
}

// ── Audio helpers ────────────────────────────────────────────────────────

pub fn decode_pcm16(delta: &str) -> Vec<i16> {
    base64::engine::general_purpose::STANDARD
        .decode(delta)
        .unwrap()
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
        .collect()
}

pub fn pcm16_base64(samples: &[i16]) -> String {
    let bytes: Vec<u8> = samples.iter().flat_map(|sample| sample.to_le_bytes()).collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Milliseconds of audio in an `audio-delta` at `rate`.
pub fn delta_ms(frame: &Value, rate: u32) -> f64 {
    decode_pcm16(frame["delta"].as_str().expect("audio-delta")).len() as f64 * 1000.0
        / f64::from(rate)
}

pub fn sine_f32(rate: u32, hz: f32, samples: usize) -> Vec<f32> {
    (0..samples)
        .map(|n| (2.0 * std::f32::consts::PI * hz * n as f32 / rate as f32).sin() * 0.5)
        .collect()
}

pub fn sine_pcm16(rate: u32, hz: f32, samples: usize) -> Vec<i16> {
    sine_f32(rate, hz, samples)
        .iter()
        .map(|sample| (sample * 32767.0) as i16)
        .collect()
}

/// Rising zero crossings: the cycles of a tone.
pub fn cycles(samples: &[f32]) -> usize {
    samples
        .windows(2)
        .filter(|pair| pair[0] <= 0.0 && pair[1] > 0.0)
        .count()
}
