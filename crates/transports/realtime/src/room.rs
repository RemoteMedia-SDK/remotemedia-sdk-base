//! The real [`MemberJoiner`]: an admitted member joins the shared pipeline
//! session of its room, the same session the room's WebRTC peers are in.
//!
//! How frames travel (the contract with `RealtimeEventsNode`):
//!
//! - The member's audio enters the session as that participant's input
//!   (`ParticipantSessionHandle::send`), so it reaches the pipeline's entry
//!   nodes like a WebRTC peer's microphone.
//! - A frame for the node is published to `<node>.in.realtime` with the
//!   member's `participant.*` metadata. The node receives
//!   `RuntimeData::Json({"__aux_port__": "realtime", "payload": <frame>,
//!   "participant.id": ..., ...})`, and the router delivers it to the
//!   member's instance of a per-participant node.
//! - A frame from the node is an output wrapped for the `realtime` port
//!   (`wrap_aux_port("realtime", RuntimeData::Json(frame))`). The transport
//!   reads it from the attributed tap `<node>.out.realtime`, with the
//!   participant the router attributed it to.
//! - Audio comes from the session's audio outputs (its sinks).

use crate::link::{MemberInput, MemberJoiner, MemberLink, Outbound};
use crate::protocol;
use crate::RealtimeConfig;
use async_trait::async_trait;
use remotemedia_core::data::{AudioSamples, RuntimeData};
use remotemedia_core::transport::data::participant;
use remotemedia_core::transport::session_control::{
    ControlAddress, RpcCallScope, SessionControl,
};
use remotemedia_core::transport::{Participant, TransportData};
use remotemedia_webrtc::signaling::websocket::{AdmittedPeer, SharedState};
use remotemedia_webrtc::SharedRoomSession;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Outputs waiting for the connection.
const OUTPUT_QUEUE: usize = 1024;

/// Joins members to the rooms of the signalling server's [`SharedState`].
pub struct RoomJoiner {
    config: RealtimeConfig,
}

impl RoomJoiner {
    pub fn new(config: RealtimeConfig) -> Self {
        Self { config }
    }
}

/// The session's `RealtimeEventsNode`: its id and the parameters the
/// transport reads.
struct EventsNode {
    id: String,
    lead_ms: Option<u64>,
    input_rate: Option<u32>,
}

fn param_u64(params: &Value, names: &[&str]) -> Option<u64> {
    names.iter().find_map(|name| params.get(name)?.as_u64())
}

fn events_node(control: &SessionControl, config: &RealtimeConfig) -> Option<EventsNode> {
    let descriptor = control.descriptor();
    let found = descriptor.as_ref().and_then(|descriptor| {
        descriptor.nodes.iter().find(|node| match &config.events_node {
            Some(id) => &node.id == id,
            None => node.node_type == config.events_node_type,
        })
    });
    match (found, &config.events_node) {
        (Some(node), _) => Some(EventsNode {
            id: node.id.clone(),
            lead_ms: param_u64(&node.params, &["lead_ms", "leadMs"]),
            input_rate: param_u64(&node.params, &["input_rate", "inputRate"])
                .map(|rate| rate as u32),
        }),
        // Named by the host: trusted without a descriptor to look it up in.
        (None, Some(id)) => Some(EventsNode {
            id: id.clone(),
            lead_ms: None,
            input_rate: None,
        }),
        (None, None) => None,
    }
}

/// What the admission said about the member, as frame attribution: each
/// scalar becomes `participant.metadata.<key>`, as for a WebRTC peer. The
/// realtime admission itself is the transport's and is left out.
fn participant_metadata(metadata: &HashMap<String, Value>) -> HashMap<String, String> {
    metadata
        .iter()
        .filter(|(key, _)| key.as_str() != "display_name" && key.as_str() != protocol::ADMISSION_KEY)
        .filter_map(|(key, value)| {
            let value = match value {
                Value::String(text) => text.clone(),
                Value::Bool(_) | Value::Number(_) => value.to_string(),
                _ => return None,
            };
            Some((format!("participant.metadata.{key}"), value))
        })
        .collect()
}

struct RoomInput {
    member: String,
    connection: String,
    room: Arc<SharedRoomSession>,
    participant: remotemedia_core::transport::ParticipantSessionHandle,
    /// `participant.*` of the member, for frames published to the node.
    attribution: HashMap<String, String>,
    control: Option<Arc<SessionControl>>,
    node: Option<String>,
    port: String,
    scope: RpcCallScope,
    forwarders: Vec<JoinHandle<()>>,
}

#[async_trait]
impl MemberInput for RoomInput {
    async fn audio(&self, samples: Vec<f32>, sample_rate: u32) -> Result<(), String> {
        let frame = TransportData::new(RuntimeData::Audio {
            samples: AudioSamples::from(samples),
            sample_rate,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        });
        self.participant
            .send(frame)
            .await
            .map_err(|error| error.to_string())
    }

    async fn event(&self, frame: Value) -> Result<(), String> {
        let (Some(control), Some(node)) = (&self.control, &self.node) else {
            return Ok(());
        };
        let address = ControlAddress::node_in(node.clone()).with_port(self.port.clone());
        control
            .publish_scoped_with_metadata(
                &address,
                RuntimeData::Json(frame),
                &self.scope,
                self.attribution.clone(),
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn leave(&self) {
        for forwarder in &self.forwarders {
            forwarder.abort();
        }
        if let Some(control) = &self.control {
            control.publish_media_lost(&self.member, protocol::TRANSPORT_NAME, &self.connection);
        }
        if let Err(error) = self.participant.leave().await {
            debug!("realtime member {} left an ended session: {}", self.member, error);
        }
        self.room.detach_member(&self.member).await;
    }
}

#[async_trait]
impl MemberJoiner for RoomJoiner {
    async fn join(
        &self,
        state: &Arc<SharedState>,
        member: &AdmittedPeer,
        connection: &str,
    ) -> Result<MemberLink, String> {
        let room = state
            .room(&member.room)
            .await
            .map_err(|error| error.to_string())?;
        let control = state.runner.control_bus().get(room.session_id());
        let node = control
            .as_deref()
            .and_then(|control| events_node(control, &self.config));
        if node.is_none() {
            warn!(
                "session {} has no {} node: realtime member {} gets audio, but no events",
                room.session_id(),
                self.config.events_node_type,
                member.peer_id
            );
        }

        let mut who = Participant::new(member.peer_id.clone(), participant::role::CLIENT)
            .with_track_id(format!("{}:realtime", member.peer_id))
            .with_modality(participant::modality::AUDIO)
            .with_metadata(protocol::TRANSPORT_KEY, protocol::TRANSPORT_NAME);
        if let Some(name) = &member.display_name {
            who = who.with_display_name(name.clone());
        }
        for (key, value) in participant_metadata(&member.metadata) {
            who = who.with_metadata(key, value);
        }
        let attribution = who.frame_metadata();
        let handle = room.participant_handle(who);

        // Subscribe before the member is announced, so nothing produced for
        // it is missed. One task forwards everything, in arrival order.
        let (out_tx, outputs) = mpsc::channel::<Outbound>(OUTPUT_QUEUE);
        let (mut audio_rx, _video, _data) = room.subscribe_outputs().split();
        let mut flush_rx = room.subscribe_flushes();
        let mut events = match (&control, &node) {
            (Some(control), Some(node)) => {
                let address = ControlAddress::node_out(node.id.clone())
                    .with_port(self.config.realtime_port.clone());
                Some(
                    control
                        .subscribe_attributed(&address)
                        .map_err(|error| error.to_string())?,
                )
            }
            _ => None,
        };
        let watched = Arc::clone(&room);
        let forwarder = tokio::spawn(async move {
            let mut alive = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                let next = tokio::select! {
                    audio = audio_rx.recv() => match audio {
                        Ok(audio) => Outbound::Audio(audio),
                        Err(broadcast::error::RecvError::Lagged(lost)) => {
                            warn!("realtime audio output lagged; {lost} frames lost");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    flush = flush_rx.recv() => match flush {
                        Ok(target) => Outbound::Flush(target),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    event = async {
                        match events.as_mut() {
                            Some(events) => events.recv().await,
                            None => std::future::pending().await,
                        }
                    } => match event {
                        Ok((who, RuntimeData::Json(envelope))) => {
                            // The port's envelope: the frame is its payload.
                            let Some(frame) = envelope.get("payload").cloned() else { continue };
                            Outbound::Event { participant: who.map(|who| who.to_string()), frame }
                        }
                        Ok(_) => continue,
                        Err(broadcast::error::RecvError::Lagged(lost)) => {
                            warn!("realtime events lagged; {lost} frames lost");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    // The session ended: the connection closes.
                    _ = alive.tick() => {
                        if watched.is_active().await { continue } else { break }
                    }
                };
                if out_tx.send(next).await.is_err() {
                    break;
                }
            }
        });
        let forwarders = vec![forwarder];

        handle.join().await.map_err(|error| error.to_string())?;
        room.attach_member(&member.peer_id);
        // The member can hear output from now on: speech held for it is sent.
        if let Some(control) = &control {
            control.publish_media_ready(&member.peer_id, protocol::TRANSPORT_NAME, connection);
        }

        Ok(MemberLink {
            lead_ms: node.as_ref().and_then(|node| node.lead_ms),
            pipeline_sample_rate: node.as_ref().and_then(|node| node.input_rate),
            input: Arc::new(RoomInput {
                member: member.peer_id.clone(),
                connection: connection.to_string(),
                room,
                participant: handle,
                attribution,
                control,
                node: node.map(|node| node.id),
                port: self.config.realtime_port.clone(),
                scope: RpcCallScope::default(),
                forwarders,
            }),
            outputs,
        })
    }
}
