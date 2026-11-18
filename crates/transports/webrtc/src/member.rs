//! A WebRTC member peer: a non-browser client that joins a runtime as a
//! member, exactly as a browser does for a voice agent (openspec:
//! extend-ai-sdk-media-streaming, design D11).
//!
//! The runtime admits the member over its control address (a single-use
//! ticket from `RuntimeControl.AdmitMember`, or Entry's `join`); the member
//! then connects to the pipeline's own transport:
//!
//! 1. WebSocket JSON-RPC signalling: `peer.announce { peer_id, ticket,
//!    capabilities: ["data"] }` first, so the ticket decides who the member
//!    is and which session it joins;
//! 2. `peer.offer` with one ordered data channel and no media tracks, asking
//!    for audio as data (`receive: { audio: "data", video: false }`), sent
//!    once ICE gathering has completed;
//! 3. the server's trickled candidates, applied for the life of the member.
//!
//! Every value in either direction is one lossless protobuf `DataBuffer` on
//! the data channel (audio as f32 little-endian), split into
//! `DataChannelMessage` chunks above [`crate::peer::DATA_CHANNEL_FRAME_LIMIT`].
//! [`MemberPeer::end_input`] tells a session admitted with `end_on_leave`
//! that the member's input is over; once the pipeline has drained the member
//! receives [`MemberEvent::Drained`]. The open socket is the membership:
//! [`MemberPeer::close`] (or dropping the peer) leaves.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use remotemedia_core::data::RuntimeData;
use remotemedia_types::{CopyContext, CopyPolicy};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::channels::{DataChannelMessage, DataChannelReassembler};
use crate::{Error, Result};

/// Label of the member's pipeline data channel (the browser client's).
pub const PIPELINE_CHANNEL_LABEL: &str = "pipeline";

/// The server peer's id in signalling.
const SERVER_PEER: &str = "remotemedia-server";

/// An ICE server for the member's connection.
#[derive(Debug, Clone, Default)]
pub struct MemberIceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

/// How to join.
#[derive(Debug, Clone)]
pub struct MemberPeerConfig {
    /// The signalling URL the admission returned (`ws://…` or `wss://…`).
    pub signal_url: String,
    /// The single-use ticket the admission returned.
    pub ticket: String,
    /// ICE servers for this member's connection. Empty: host candidates only.
    pub ice_servers: Vec<MemberIceServer>,
    /// How long signalling, ICE and DTLS may take before `connect` fails.
    pub connect_timeout: Duration,
}

impl MemberPeerConfig {
    pub fn new(signal_url: impl Into<String>, ticket: impl Into<String>) -> Self {
        Self {
            signal_url: signal_url.into(),
            ticket: ticket.into(),
            ice_servers: Vec::new(),
            connect_timeout: Duration::from_secs(20),
        }
    }
}

/// What the member receives.
#[derive(Debug, Clone)]
pub enum MemberEvent {
    /// One output of the session.
    Output(RuntimeData),
    /// The session drained after every member ended its input; no output
    /// follows.
    Drained,
    /// The connection ended, with the reason when one is known.
    Closed(Option<String>),
}

/// See the module documentation.
pub struct MemberPeer {
    peer_id: String,
    rtc: Arc<RTCPeerConnection>,
    channel: Arc<RTCDataChannel>,
    events: Mutex<mpsc::Receiver<MemberEvent>>,
    signalling: Mutex<Option<tokio::task::JoinHandle<()>>>,
    socket_out: mpsc::Sender<Message>,
    copies: CopyContext,
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Next JSON-RPC response with `id`; notifications before it are queued in
/// `pending` for the signalling task.
async fn response(ws: &mut Ws, id: u64, pending: &mut Vec<Value>) -> Result<Value> {
    loop {
        let Some(message) = ws.next().await else {
            return Err(Error::SignalingError(
                "the signalling server closed the connection".into(),
            ));
        };
        let message =
            message.map_err(|e| Error::SignalingError(format!("signalling socket: {e}")))?;
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)
            .map_err(|e| Error::SignalingError(format!("signalling message: {e}")))?;
        if value.get("id") == Some(&json!(id)) {
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("refused")
                    .to_string();
                return Err(Error::SignalingError(message));
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
        pending.push(value);
    }
}

async fn request(
    ws: &mut Ws,
    method: &str,
    params: Value,
    id: u64,
    pending: &mut Vec<Value>,
) -> Result<Value> {
    ws.send(Message::Text(
        json!({ "jsonrpc": "2.0", "method": method, "params": params, "id": id }).to_string(),
    ))
    .await
    .map_err(|e| Error::SignalingError(format!("signalling socket: {e}")))?;
    response(ws, id, pending).await
}

/// Apply a `peer.ice_candidate` notification from the server.
async fn apply_candidate(rtc: &RTCPeerConnection, value: &Value) {
    if value.get("method").and_then(Value::as_str) != Some("peer.ice_candidate") {
        return;
    }
    let params = &value["params"];
    let candidate = params["candidate"].as_str().unwrap_or_default();
    if candidate.is_empty() {
        return;
    }
    if let Err(e) = rtc
        .add_ice_candidate(RTCIceCandidateInit {
            candidate: candidate.to_string(),
            sdp_mid: params["sdp_mid"].as_str().map(str::to_string),
            sdp_mline_index: params["sdp_m_line_index"].as_u64().map(|i| i as u16),
            username_fragment: None,
        })
        .await
    {
        tracing::debug!("member peer: server candidate not applied: {e}");
    }
}

/// The drained event, by its content.
fn is_drained(data: &RuntimeData) -> bool {
    matches!(data, RuntimeData::Json(value)
        if value.get("type").and_then(Value::as_str)
            == Some(remotemedia_core::transport::shared_session::DRAINED_EVENT_TYPE))
}

impl MemberPeer {
    /// Join with a ticket. Returns once the data channel is open (ICE and
    /// DTLS have succeeded); fails with the server's reason when the ticket
    /// is refused, or when it takes longer than `connect_timeout`.
    pub async fn connect(config: MemberPeerConfig) -> Result<Self> {
        let deadline = config.connect_timeout;
        tokio::time::timeout(deadline, Self::connect_inner(config))
            .await
            .map_err(|_| {
                Error::SignalingError(format!(
                    "the member peer did not connect within {} ms",
                    deadline.as_millis()
                ))
            })?
    }

    async fn connect_inner(config: MemberPeerConfig) -> Result<Self> {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "member-peer");
        let peer_id = format!("member-{}", uuid::Uuid::new_v4().simple());
        let (mut ws, _) = tokio_tungstenite::connect_async(config.signal_url.as_str())
            .await
            .map_err(|e| {
                Error::SignalingError(format!("connecting to {}: {e}", config.signal_url))
            })?;
        let mut pending = Vec::new();
        let announced = request(
            &mut ws,
            "peer.announce",
            json!({
                "peer_id": peer_id,
                "ticket": config.ticket,
                "capabilities": ["data"],
                "user_data": {},
            }),
            1,
            &mut pending,
        )
        .await?;
        // The ticket may have given the member another id.
        let peer_id = announced
            .get("peer_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or(peer_id);

        let rtc = Arc::new(
            webrtc::api::APIBuilder::new()
                .build()
                .new_peer_connection(RTCConfiguration {
                    ice_servers: config
                        .ice_servers
                        .iter()
                        .map(|server| RTCIceServer {
                            urls: server.urls.clone(),
                            username: server.username.clone().unwrap_or_default(),
                            credential: server.credential.clone().unwrap_or_default(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::WebRtcError(format!("peer connection: {e}")))?,
        );
        let channel = rtc
            .create_data_channel(PIPELINE_CHANNEL_LABEL, None)
            .await
            .map_err(|e| Error::WebRtcError(format!("data channel: {e}")))?;

        let (events_tx, events) = mpsc::channel::<MemberEvent>(256);
        let (opened_tx, mut opened_rx) = mpsc::channel::<()>(1);
        channel.on_open(Box::new(move || {
            let opened_tx = opened_tx.clone();
            Box::pin(async move {
                let _ = opened_tx.send(()).await;
            })
        }));
        let reassembler = Arc::new(Mutex::new(DataChannelReassembler::new()));
        let output_tx = events_tx.clone();
        let output_copies = copies.clone();
        channel.on_message(Box::new(move |message| {
            let output_tx = output_tx.clone();
            let reassembler = Arc::clone(&reassembler);
            let copies = output_copies.clone();
            Box::pin(async move {
                let decoded = {
                    let mut pending = reassembler.lock().await;
                    crate::peer::decode_pipeline_input(message.data, &mut pending, &copies)
                };
                let event = match decoded {
                    Ok(Some(data)) if is_drained(&data) => MemberEvent::Drained,
                    Ok(Some(data)) => MemberEvent::Output(data),
                    Ok(None) => return,
                    Err(e) => {
                        tracing::warn!("member peer: output not decoded: {e}");
                        return;
                    }
                };
                let _ = output_tx.send(event).await;
            })
        }));
        let closed_tx = events_tx.clone();
        channel.on_close(Box::new(move || {
            let closed_tx = closed_tx.clone();
            Box::pin(async move {
                let _ =
                    closed_tx.try_send(MemberEvent::Closed(Some("the data channel closed".into())));
            })
        }));

        let mut gathered = rtc.gathering_complete_promise().await;
        let offer = rtc
            .create_offer(None)
            .await
            .map_err(|e| Error::WebRtcError(format!("offer: {e}")))?;
        rtc.set_local_description(offer)
            .await
            .map_err(|e| Error::WebRtcError(format!("local description: {e}")))?;
        gathered.recv().await;
        let sdp = rtc
            .local_description()
            .await
            .ok_or_else(|| Error::WebRtcError("no local description".into()))?
            .sdp;
        let answer = request(
            &mut ws,
            "peer.offer",
            json!({
                "from": peer_id,
                "to": SERVER_PEER,
                "sdp": sdp,
                "receive": { "audio": "data", "video": false },
            }),
            2,
            &mut pending,
        )
        .await?;
        let answer_sdp = answer.get("sdp").and_then(Value::as_str).ok_or_else(|| {
            Error::SignalingError(format!("peer.offer answered without sdp: {answer}"))
        })?;
        rtc.set_remote_description(
            RTCSessionDescription::answer(answer_sdp.to_string())
                .map_err(|e| Error::WebRtcError(format!("answer: {e}")))?,
        )
        .await
        .map_err(|e| Error::WebRtcError(format!("remote description: {e}")))?;
        for value in pending.drain(..) {
            apply_candidate(&rtc, &value).await;
        }

        // The signalling task: server candidates in, `close` out. The socket
        // closing ends the membership.
        let (socket_out, mut socket_rx) = mpsc::channel::<Message>(8);
        let trickle = Arc::clone(&rtc);
        let closed_tx = events_tx;
        let signalling = tokio::spawn(async move {
            let (mut sink, mut stream) = ws.split();
            loop {
                tokio::select! {
                    outgoing = socket_rx.recv() => match outgoing {
                        Some(message) => {
                            let close = matches!(message, Message::Close(_));
                            let _ = sink.send(message).await;
                            if close {
                                break;
                            }
                        }
                        None => {
                            let _ = sink.close().await;
                            break;
                        }
                    },
                    incoming = stream.next() => match incoming {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                apply_candidate(&trickle, &value).await;
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            let reason = frame.map(|f| f.reason.to_string()).filter(|r| !r.is_empty());
                            let _ = closed_tx.try_send(MemberEvent::Closed(reason.or_else(|| Some("the signalling connection closed".into()))));
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            let _ = closed_tx.try_send(MemberEvent::Closed(Some(format!("signalling socket: {e}"))));
                            break;
                        }
                        None => {
                            let _ = closed_tx.try_send(MemberEvent::Closed(Some("the signalling connection closed".into())));
                            break;
                        }
                    },
                }
            }
        });

        if opened_rx.recv().await.is_none() {
            signalling.abort();
            return Err(Error::WebRtcError("the data channel did not open".into()));
        }
        Ok(Self {
            peer_id,
            rtc,
            channel,
            events: Mutex::new(events),
            signalling: Mutex::new(Some(signalling)),
            socket_out,
            copies,
        })
    }

    /// The member's peer id on the runtime.
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Send one value to the session.
    pub async fn send(&self, data: &RuntimeData) -> Result<()> {
        let encoded = crate::adapters::encode_runtime_data_with_context(data, &self.copies)
            .map_err(|e| Error::InvalidData(format!("encoding input: {e}")))?;
        crate::peer::send_framed(&self.channel, encoded).await
    }

    /// The member's input is over. In a session admitted with
    /// `end_on_leave`, once every member has said so, the session drains and
    /// [`MemberEvent::Drained`] follows its last output.
    pub async fn end_input(&self) -> Result<()> {
        let message = DataChannelMessage::Control {
            action: crate::peer::END_OF_INPUT_ACTION.to_string(),
            headers: Default::default(),
            json: Value::Null,
        }
        .encode()
        .map_err(|e| Error::Other(e.into()))?;
        self.channel
            .send(&Bytes::from(message))
            .await
            .map(|_| ())
            .map_err(|e| Error::WebRtcError(format!("Data channel send failed: {e}")))
    }

    /// The next event; `None` once the member is closed and every event has
    /// been read.
    pub async fn recv(&self) -> Option<MemberEvent> {
        self.events.lock().await.recv().await
    }

    /// Leave: close the data channel, the peer connection and the
    /// signalling socket.
    pub async fn close(&self) {
        let _ = self.socket_out.send(Message::Close(None)).await;
        let _ = self.channel.close().await;
        let _ = self.rtc.close().await;
        if let Some(task) = self.signalling.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }
}

impl Drop for MemberPeer {
    fn drop(&mut self) {
        if let Ok(mut task) = self.signalling.try_lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
        let rtc = Arc::clone(&self.rtc);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = rtc.close().await;
            });
        }
    }
}
