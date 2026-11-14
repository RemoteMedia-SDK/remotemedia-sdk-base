//! Realtime WebSocket transport (openspec: add-ai-sdk-voice-library, design
//! D10, tasks 9.2 and 9.5).
//!
//! A browser running the AI SDK's `experimental_useRealtime` connects here
//! and talks to a voice agent with the SDK's normalized realtime events as
//! JSON frames. The split is the telephony transport's: this crate owns the
//! socket, admission and the clock; a pipeline node (`RealtimeEventsNode`)
//! owns the meaning of the events.
//!
//! The transport:
//! - is served at [`REALTIME_PATH`] on the WebRTC signalling listener
//!   ([`mount`]), and admits members with the listener's own single-use
//!   tickets, carried as a WebSocket subprotocol and never in the URL;
//! - joins the member to its room's session, sends `session-created`, and
//!   tells the member's node `remotemedia.internal.joined`;
//! - turns `input-audio-append` into pipeline audio and the session's audio
//!   into paced `audio-delta` frames, resampling both ways;
//! - reports what it released per item (`remotemedia.internal.released`) and
//!   drops an item's queued audio on `remotemedia.internal.flush`;
//! - applies the session-configuration policy to `session-update`;
//! - passes every other frame, unchanged, between the socket and the
//!   `realtime` port of the member's node;
//! - shows a connection only its own member's frames.
//!
//! See [`room`] for how frames travel to and from the node.

pub mod admission;
pub mod audio;
pub mod connection;
pub mod link;
pub mod pacer;
pub mod protocol;
pub mod room;

pub use admission::{FilteredConfig, RealtimeAdmission};
pub use link::{MemberInput, MemberJoiner, MemberLink, Outbound};
pub use protocol::{REALTIME_PATH, SUBPROTOCOL, TICKET_SUBPROTOCOL_PREFIX};
pub use room::RoomJoiner;

use remotemedia_webrtc::signaling::websocket::{
    SharedState, SocketAccept, SocketRefusal, SocketRequest, SocketRoute,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Notify};

/// Settings of the realtime socket. A session's `RealtimeEventsNode`
/// parameters (`lead_ms`, `input_rate`) win over `lead_ms` and
/// `pipeline_sample_rate` here.
#[derive(Debug, Clone)]
pub struct RealtimeConfig {
    /// How far ahead of real time audio is sent (`realtime.leadMs`).
    pub lead_ms: u64,
    /// The smallest piece of audio sent as a frame of its own: once the lead
    /// is used up, one `audio-delta` of this length per this interval.
    pub send_interval_ms: u64,
    /// The longest time between `released` reports while audio is sent.
    pub release_report_ms: u64,
    /// The rate member audio enters the pipeline at. 48 kHz is what the
    /// WebRTC transport feeds the same pipelines.
    pub pipeline_sample_rate: u32,
    /// The session's input and output rates until a `session-update` names
    /// them.
    pub default_input_rate: u32,
    pub default_output_rate: u32,
    /// The id of the node whose `realtime` port the transport talks to.
    /// `None`: the session's node of type [`Self::events_node_type`].
    pub events_node: Option<String>,
    pub events_node_type: String,
    pub realtime_port: String,
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        Self {
            lead_ms: 500,
            send_interval_ms: 20,
            release_report_ms: 100,
            pipeline_sample_rate: 48_000,
            default_input_rate: 24_000,
            default_output_rate: 24_000,
            events_node: None,
            events_node_type: protocol::EVENTS_NODE_TYPE.to_string(),
            realtime_port: protocol::REALTIME_PORT.to_string(),
        }
    }
}

/// A member's live connection: how to end it, and whether it has left.
struct Live {
    connection: String,
    replaced: Arc<Notify>,
    left: watch::Receiver<bool>,
}

/// The realtime socket as a route of the signalling listener.
pub struct RealtimeRoute {
    config: RealtimeConfig,
    joiner: Arc<dyn MemberJoiner>,
    /// One connection per member: a newer one replaces the earlier.
    live: Arc<Mutex<HashMap<String, Live>>>,
    next_connection: AtomicU64,
}

impl RealtimeRoute {
    /// The route for the rooms of the signalling server.
    pub fn new(config: RealtimeConfig) -> Self {
        let joiner = Arc::new(RoomJoiner::new(config.clone()));
        Self::with_joiner(config, joiner)
    }

    /// The route with another way of joining members to sessions.
    pub fn with_joiner(config: RealtimeConfig, joiner: Arc<dyn MemberJoiner>) -> Self {
        Self {
            config,
            joiner,
            live: Arc::new(Mutex::new(HashMap::new())),
            next_connection: AtomicU64::new(1),
        }
    }
}

impl SocketRoute for RealtimeRoute {
    fn accept(
        &self,
        state: &Arc<SharedState>,
        request: &SocketRequest,
    ) -> Result<SocketAccept, SocketRefusal> {
        let refuse = |status: u16, reason: &str| SocketRefusal {
            status,
            reason: reason.to_string(),
        };
        if !request.protocols.iter().any(|protocol| protocol == SUBPROTOCOL) {
            return Err(refuse(400, "the realtime socket speaks remotemedia.realtime.v1"));
        }
        // Only the subprotocol carries a ticket. The URL is not looked at.
        let Some(ticket) = protocol::ticket_from_protocols(&request.protocols) else {
            return Err(refuse(
                401,
                "no ticket: offer it as the subprotocol remotemedia.ticket.<ticket>",
            ));
        };
        // The same admission as `peer.announce`: a single-use ticket is
        // consumed here, before the socket opens.
        let admitted = match state.admit(Some(ticket)) {
            Some(Ok(admitted)) => admitted,
            Some(Err(reason)) => return Err(refuse(401, &format!("not admitted: {reason}"))),
            None => return Err(refuse(403, "this listener admits no realtime members")),
        };

        let connection = format!(
            "rt-{}-{}",
            admitted.peer_id,
            self.next_connection.fetch_add(1, Ordering::Relaxed)
        );
        let config = self.config.clone();
        let joiner = Arc::clone(&self.joiner);
        let live = Arc::clone(&self.live);
        let state = Arc::clone(state);
        Ok(SocketAccept {
            protocol: Some(SUBPROTOCOL.to_string()),
            serve: Box::new(move |socket| {
                Box::pin(async move {
                    let member = admitted.peer_id.clone();
                    // A member has one connection. The earlier one ends, and
                    // has left the session before this one joins it.
                    let replaced = Arc::new(Notify::new());
                    let (left_tx, left) = watch::channel(false);
                    let earlier = live.lock().expect("live connections").insert(
                        member.clone(),
                        Live {
                            connection: connection.clone(),
                            replaced: Arc::clone(&replaced),
                            left,
                        },
                    );
                    if let Some(mut earlier) = earlier {
                        earlier.replaced.notify_one();
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            earlier.left.wait_for(|left| *left),
                        )
                        .await;
                    }

                    match joiner.join(&state, &admitted, &connection).await {
                        Ok(link) => {
                            let admission = RealtimeAdmission::from_metadata(&admitted.metadata);
                            connection::run(
                                socket,
                                member.clone(),
                                admission,
                                link,
                                config,
                                connection.clone(),
                                replaced,
                            )
                            .await;
                        }
                        Err(reason) => {
                            tracing::warn!("realtime member {} could not join: {}", member, reason);
                            connection::refuse(socket, "the session is not available").await;
                        }
                    }

                    let mut live = live.lock().expect("live connections");
                    if live.get(&member).is_some_and(|entry| entry.connection == connection) {
                        live.remove(&member);
                    }
                    drop(live);
                    let _ = left_tx.send(true);
                })
            }),
        })
    }
}

/// Serve the realtime socket at [`REALTIME_PATH`] on the signalling listener
/// that owns `state`. The listener needs an admission
/// (`SharedState::set_admission`): the socket admits ticket holders only.
pub fn mount(state: &Arc<SharedState>, config: RealtimeConfig) {
    state.mount(REALTIME_PATH, Arc::new(RealtimeRoute::new(config)));
}

/// [`mount`] with another way of joining members to sessions.
pub fn mount_with(state: &Arc<SharedState>, config: RealtimeConfig, joiner: Arc<dyn MemberJoiner>) {
    state.mount(
        REALTIME_PATH,
        Arc::new(RealtimeRoute::with_joiner(config, joiner)),
    );
}
