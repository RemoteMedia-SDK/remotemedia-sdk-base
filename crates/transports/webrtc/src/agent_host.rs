//! Sessions by application key and admission by single-use ticket on a
//! WebSocket signalling server (openspec `add-ai-sdk-voice-library`, task
//! 6.2).
//!
//! An [`AgentHost`] turns a signalling server into one an application's
//! backend admits members to:
//!
//! 1. a pipeline manifest is registered under a name;
//! 2. the backend asks for a ticket for a member of `(pipeline, session
//!    key)`. Members admitted with the same key share one session, each
//!    attributed with its admitted identity (`participant.id`,
//!    `participant.display_name`, `participant.metadata.<key>`);
//! 3. the session's control bus is the executor's, so the backend attaches
//!    to it the usual way;
//! 4. watchers are told when sessions start and end and when members join
//!    and leave ([`AgentHost::subscribe`]).
//!
//! A browser joins only with a ticket. Its identity, name, metadata and
//! control grants are the ticket's; what it says about itself is ignored.
//! With no grants it gets media and the pipeline data channel and nothing
//! else.
//!
//! `remotemedia serve --transport webrtc --admission tickets` runs one behind
//! the gRPC control API. The embedded Node server (`WebRtcAgentServer` in the
//! SDK's `remotemedia-ffi`) implements the same rules over the same ticket
//! store ([`crate::agent_tickets`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use remotemedia_core::manifest::Manifest;
use tokio::sync::{broadcast, mpsc};

use crate::agent_tickets::{self, Admission, TicketGrant, TicketStore};
use crate::control::{ControlAccess, ControlGrant, ControlOp};
use crate::signaling::websocket::{AdmittedPeer, PeerAdmission, SharedState, WebRtcEventBridge};
use crate::SharedRoomSession;

/// How long a session with no member and no usable ticket is kept, unless
/// the configuration says otherwise.
pub const DEFAULT_EMPTY_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration of an [`AgentHost`].
#[derive(Debug, Clone)]
pub struct AgentHostConfig {
    /// The signalling URL browsers connect to with their ticket.
    pub signal_url: String,
    /// How long a session is kept with no member and no usable ticket before
    /// it ends with reason `empty`.
    pub empty_session_timeout: Duration,
}

/// A member of a session, as it was admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMember {
    pub id: String,
    pub name: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// A live session of the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionInfo {
    pub pipeline: String,
    /// The application's session key.
    pub session_key: String,
    /// The runtime's session id.
    pub session_id: String,
}

/// What happened to a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSessionEventKind {
    SessionStarted,
    /// `reason`: `ended` | `empty` | `closed` | `shutdown`, or the reason
    /// given to [`AgentHost::end_session`].
    SessionEnded,
    MemberJoined,
    /// `reason`: `disconnected` | `replaced` | `removed` | `session_ended`.
    MemberLeft,
}

/// A session lifecycle notification.
#[derive(Debug, Clone)]
pub struct AgentSessionEvent {
    pub kind: AgentSessionEventKind,
    pub session: AgentSessionInfo,
    /// The member, for `MemberJoined` and `MemberLeft`.
    pub member: Option<AgentMember>,
    pub reason: Option<String>,
}

/// Who to admit, and to which session.
#[derive(Debug, Clone)]
pub struct AgentTicketRequest {
    /// Name of a registered pipeline.
    pub pipeline: String,
    /// The application's session key.
    pub session_key: String,
    pub member: AgentMember,
    /// Lifetime of the ticket.
    pub ttl: Duration,
    /// Control grants. Empty: media and the pipeline data channel only.
    pub grants: Vec<TicketGrant>,
}

/// A minted ticket.
#[derive(Debug, Clone)]
pub struct AgentTicket {
    /// Present it as `ticket` in `peer.announce`. Valid once.
    pub ticket: String,
    /// The runtime id of the session the member will join.
    pub session_id: String,
    /// When the ticket stops being accepted.
    pub expires: SystemTime,
}

/// Why the host refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentHostError {
    /// No pipeline of that name is registered. Carries the registered names.
    UnknownPipeline {
        pipeline: String,
        registered: Vec<String>,
    },
    /// The request is not one a ticket can be minted for.
    Invalid(String),
    /// The session could not be started or served.
    Session(String),
    /// The host has shut down.
    Stopped,
}

impl std::fmt::Display for AgentHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentHostError::UnknownPipeline {
                pipeline,
                registered,
            } => write!(
                f,
                "pipeline '{pipeline}' is not served by this runtime (it serves: {})",
                if registered.is_empty() {
                    "none".to_string()
                } else {
                    registered.join(", ")
                }
            ),
            AgentHostError::Invalid(reason) | AgentHostError::Session(reason) => {
                write!(f, "{reason}")
            }
            AgentHostError::Stopped => write!(f, "the agent host has shut down"),
        }
    }
}

impl std::error::Error for AgentHostError {}

struct Session {
    info: AgentSessionInfo,
    room: Arc<SharedRoomSession>,
    members: HashMap<String, AgentMember>,
    /// Since when the session has had no member.
    empty_since: Instant,
}

impl Session {
    fn event(
        &self,
        kind: AgentSessionEventKind,
        member: Option<AgentMember>,
        reason: Option<String>,
    ) -> AgentSessionEvent {
        AgentSessionEvent {
            kind,
            session: self.info.clone(),
            member,
            reason,
        }
    }
}

/// A member whose ticket was accepted, and the room it was admitted to.
#[derive(Clone)]
struct MemberRecord {
    room: String,
    member: AgentMember,
}

/// The ticket table as the signalling server's admission.
struct Tickets {
    store: TicketStore,
    /// Members whose ticket was accepted and whose arrival has not been
    /// reported yet, by member id.
    admitted: Mutex<HashMap<String, MemberRecord>>,
}

/// The control access a ticket's grants give.
pub fn control_access(grants: &[TicketGrant]) -> ControlAccess {
    ControlAccess::Granted(
        grants
            .iter()
            .filter_map(|grant| {
                let mut out = ControlGrant::new(ControlOp::parse(&grant.op)?, grant.node.clone());
                if grant.own {
                    out = out.own();
                }
                Some(out.methods(grant.methods.iter().cloned()))
            })
            .collect(),
    )
}

impl PeerAdmission for Tickets {
    fn admit(&self, ticket: Option<&str>) -> Result<AdmittedPeer, String> {
        let admission = self
            .store
            .redeem(ticket, SystemTime::now())
            .map_err(|refused| refused.to_string())?;
        // Shown to the room's other peers, and stamped on the member's input
        // as `participant.metadata.<key>` by the signalling server.
        let mut metadata: HashMap<String, serde_json::Value> = admission
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        if let Some(name) = &admission.name {
            metadata.insert("display_name".to_string(), serde_json::json!(name));
        }
        self.admitted.lock().expect("admitted members").insert(
            admission.member.clone(),
            MemberRecord {
                room: admission.room.clone(),
                member: AgentMember {
                    id: admission.member.clone(),
                    name: admission.name.clone(),
                    metadata: admission.metadata.clone(),
                },
            },
        );
        Ok(AdmittedPeer {
            peer_id: admission.member,
            room: admission.room,
            display_name: admission.name,
            metadata,
            control: control_access(&admission.grants),
        })
    }
}

struct Inner {
    state: Arc<SharedState>,
    tickets: Arc<Tickets>,
    config: AgentHostConfig,
    pipelines: Mutex<HashMap<String, Arc<Manifest>>>,
    /// Live sessions by room key.
    sessions: Mutex<HashMap<String, Session>>,
    /// Connected members by member id.
    connected: Mutex<HashMap<String, MemberRecord>>,
    /// Members the host is removing, and the reason their departure reports.
    removing: Mutex<HashMap<String, String>>,
    /// Serialises session creation, ticket issue and session end, so a
    /// ticket is never issued for a session that is ending.
    lifecycle: tokio::sync::Mutex<()>,
    events: broadcast::Sender<AgentSessionEvent>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl Inner {
    fn emit(&self, event: AgentSessionEvent) {
        let _ = self.events.send(event);
    }

    fn room_of_session(&self, session_id: &str) -> Option<String> {
        self.sessions
            .lock()
            .expect("sessions")
            .iter()
            .find(|(_, session)| session.info.session_id == session_id)
            .map(|(room, _)| room.clone())
    }

    fn check_running(&self) -> Result<(), AgentHostError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(AgentHostError::Stopped);
        }
        Ok(())
    }

    /// The live session of `(pipeline, key)`, created when there is none.
    /// Call with the lifecycle lock held.
    async fn ensure_session(
        self: &Arc<Self>,
        pipeline: &str,
        key: &str,
    ) -> Result<String, AgentHostError> {
        let room_key = agent_tickets::room_key(pipeline, key);
        let existing = self
            .sessions
            .lock()
            .expect("sessions")
            .get(&room_key)
            .map(|session| (session.info.session_id.clone(), Arc::clone(&session.room)));
        if let Some((session_id, room)) = existing {
            if room.is_active().await {
                return Ok(session_id);
            }
            // The pipeline ended on its own; a new session takes the key.
            self.end_session(&room_key, "closed").await;
        }

        let manifest = {
            let pipelines = self.pipelines.lock().expect("pipelines");
            pipelines.get(pipeline).cloned().ok_or_else(|| {
                let mut registered: Vec<String> = pipelines.keys().cloned().collect();
                registered.sort();
                AgentHostError::UnknownPipeline {
                    pipeline: pipeline.to_string(),
                    registered,
                }
            })?
        };
        let core = self
            .state
            .runner
            .get_or_create_shared_session(format!("agent-session:{room_key}"), manifest)
            .await
            .map_err(|e| {
                AgentHostError::Session(format!(
                    "Failed to start a session of pipeline '{pipeline}': {e}"
                ))
            })?;
        let session_id = core.session_id().to_string();
        let control = self.state.runner.control_bus().get(&session_id);
        // Members on the realtime socket are not signalling peers: their
        // joining and leaving is read from the session's media events.
        if let Some(control) = &control {
            let mut media = control.subscribe_media();
            let weak = Arc::downgrade(self);
            tokio::spawn(async move {
                use remotemedia_core::transport::turn_audio::MediaEvent;
                use tokio::sync::broadcast::error::RecvError;
                loop {
                    let event = match media.recv().await {
                        Ok(data) => MediaEvent::from_runtime(&data),
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => break,
                    };
                    let Some(inner) = weak.upgrade() else { break };
                    match event {
                        Some(MediaEvent::MediaReady {
                            member, transport, ..
                        }) if transport == "realtime" => {
                            inner.member_connected(&member);
                        }
                        Some(MediaEvent::MediaLost {
                            member, transport, ..
                        }) if transport == "realtime" => {
                            inner.member_disconnected(&member, None);
                        }
                        _ => {}
                    }
                }
            });
        }
        // The host owns the session: it does not close when its last
        // member leaves (the reaper ends it after the empty timeout).
        let room = SharedRoomSession::new(core, control, false)
            .await
            .map_err(|e| {
                AgentHostError::Session(format!("Failed to serve session {session_id}: {e}"))
            })?;
        self.state.host_room(&room_key, Arc::clone(&room)).await;

        let session = Session {
            info: AgentSessionInfo {
                pipeline: pipeline.to_string(),
                session_key: key.to_string(),
                session_id: session_id.clone(),
            },
            room,
            members: HashMap::new(),
            empty_since: Instant::now(),
        };
        let started = session.event(AgentSessionEventKind::SessionStarted, None, None);
        self.sessions
            .lock()
            .expect("sessions")
            .insert(room_key, session);
        self.emit(started);
        Ok(session_id)
    }

    /// End the session served as `room_key`: its unused tickets are
    /// forgotten, its members are disconnected and its pipeline session is
    /// closed. `false` when there is no such session. Call with the
    /// lifecycle lock held.
    async fn end_session(&self, room_key: &str, reason: &str) -> bool {
        let Some(mut session) = self.sessions.lock().expect("sessions").remove(room_key) else {
            return false;
        };
        self.tickets.store.revoke_room(room_key);
        // The key stays reserved on the signalling server, so a join that
        // races the end is refused instead of given a session.
        let _ = self.state.end_room(room_key).await;
        let members: Vec<(String, AgentMember)> = session.members.drain().collect();
        for (member_id, member) in members {
            // Reported here; the connection's own close is then not ours.
            self.connected
                .lock()
                .expect("connected members")
                .remove(&member_id);
            self.emit(session.event(
                AgentSessionEventKind::MemberLeft,
                Some(member),
                Some("session_ended".to_string()),
            ));
            self.drop_connection(&member_id).await;
        }
        // Watchers of the control bus (`WatchSessions`) are told why the
        // session ended: it leaves the bus here, with the reason, before its
        // pipeline is closed (which would announce it without one).
        self.state
            .runner
            .control_bus()
            .unregister_with(&session.info.session_id, Some(reason.to_string()));
        if let Err(e) = session.room.close().await {
            tracing::warn!(session_id = %session.info.session_id, "closing agent session: {e}");
        }
        self.emit(session.event(
            AgentSessionEventKind::SessionEnded,
            None,
            Some(reason.to_string()),
        ));
        true
    }

    /// End `member_id`'s signalling connection and its server peer.
    async fn drop_connection(&self, member_id: &str) {
        self.state.disconnect_peer(member_id).await;
        let server_peer = self.state.server_peers.write().await.remove(member_id);
        if let Some(server_peer) = server_peer {
            if let Err(e) = server_peer.shutdown().await {
                tracing::warn!(member = %member_id, "shutting down removed member: {e}");
            }
        }
    }

    /// A member's `peer.announce` (or realtime socket) was accepted.
    fn member_connected(&self, peer_id: &str) {
        let Some(record) = self
            .tickets
            .admitted
            .lock()
            .expect("admitted members")
            .remove(peer_id)
        else {
            return;
        };
        let event = {
            let mut sessions = self.sessions.lock().expect("sessions");
            let Some(session) = sessions.get_mut(&record.room) else {
                return;
            };
            session
                .members
                .insert(peer_id.to_string(), record.member.clone());
            session.event(
                AgentSessionEventKind::MemberJoined,
                Some(record.member.clone()),
                None,
            )
        };
        self.connected
            .lock()
            .expect("connected members")
            .insert(peer_id.to_string(), record);
        self.emit(event);
    }

    /// A member's connection ended.
    fn member_disconnected(&self, peer_id: &str, transport_reason: Option<&str>) {
        let Some(record) = self
            .connected
            .lock()
            .expect("connected members")
            .remove(peer_id)
        else {
            return;
        };
        let reason = self
            .removing
            .lock()
            .expect("removing members")
            .remove(peer_id)
            .unwrap_or_else(|| match transport_reason {
                Some(reason) if reason.starts_with("replaced") => "replaced".to_string(),
                _ => "disconnected".to_string(),
            });
        let event = {
            let mut sessions = self.sessions.lock().expect("sessions");
            let Some(session) = sessions.get_mut(&record.room) else {
                return;
            };
            session.members.remove(peer_id);
            if session.members.is_empty() {
                session.empty_since = Instant::now();
            }
            session.event(
                AgentSessionEventKind::MemberLeft,
                Some(record.member),
                Some(reason),
            )
        };
        self.emit(event);
    }

    /// End sessions whose pipeline stopped, and sessions nobody is in or
    /// can still join.
    async fn reap(&self) {
        let candidates: Vec<(String, Arc<SharedRoomSession>, bool, Instant)> = self
            .sessions
            .lock()
            .expect("sessions")
            .iter()
            .map(|(room, session)| {
                (
                    room.clone(),
                    Arc::clone(&session.room),
                    session.members.is_empty(),
                    session.empty_since,
                )
            })
            .collect();
        let timeout = self.config.empty_session_timeout;
        for (room_key, room, empty, empty_since) in candidates {
            if !room.is_active().await {
                let _guard = self.lifecycle.lock().await;
                self.end_session(&room_key, "closed").await;
                continue;
            }
            if !empty || empty_since.elapsed() < timeout {
                continue;
            }
            let _guard = self.lifecycle.lock().await;
            // Checked again under the lock: a ticket may have been minted,
            // or a member may have joined, since the snapshot.
            let still_empty = self
                .sessions
                .lock()
                .expect("sessions")
                .get(&room_key)
                .map(|session| {
                    session.members.is_empty() && session.empty_since.elapsed() >= timeout
                })
                .unwrap_or(false);
            if still_empty
                && self
                    .tickets
                    .store
                    .pending_for_room(&room_key, SystemTime::now())
                    == 0
            {
                self.end_session(&room_key, "empty").await;
            }
        }
    }
}

fn new_ticket() -> String {
    // Two v4 UUIDs: 244 random bits, from the operating system's generator.
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Sessions by application key and admission by single-use ticket, on one
/// signalling server. Cheap to clone.
#[derive(Clone)]
pub struct AgentHost {
    inner: Arc<Inner>,
}

impl AgentHost {
    /// Take over admission on the signalling server that owns `state`: from
    /// now on nobody joins it without a ticket minted here. `bridge` is the
    /// receiving end of the event channel the server was built with
    /// (`WebSocketSignalingServer::new_with_events`); the host drains it.
    /// Sessions are created on `state.runner`. Call inside a Tokio runtime,
    /// before the server starts.
    pub fn install(
        state: Arc<SharedState>,
        mut bridge: mpsc::Receiver<WebRtcEventBridge>,
        config: AgentHostConfig,
    ) -> Self {
        let tickets = Arc::new(Tickets {
            store: TicketStore::new(),
            admitted: Mutex::new(HashMap::new()),
        });
        state.set_admission(Arc::clone(&tickets) as Arc<dyn PeerAdmission>);
        let inner = Arc::new(Inner {
            state,
            tickets,
            config,
            pipelines: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            connected: Mutex::new(HashMap::new()),
            removing: Mutex::new(HashMap::new()),
            lifecycle: tokio::sync::Mutex::new(()),
            events: broadcast::channel(1024).0,
            tasks: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        });

        // Members joining and leaving, from the signalling server. Pipeline
        // output and raw data events are dropped: media stays in the runtime.
        let weak: Weak<Inner> = Arc::downgrade(&inner);
        let bridge_task = tokio::spawn(async move {
            while let Some(event) = bridge.recv().await {
                let Some(inner) = weak.upgrade() else { break };
                match event {
                    WebRtcEventBridge::PeerConnected { peer_id, .. } => {
                        inner.member_connected(&peer_id);
                    }
                    WebRtcEventBridge::PeerDisconnected { peer_id, reason } => {
                        inner.member_disconnected(&peer_id, reason.as_deref());
                    }
                    _ => {}
                }
            }
        });

        let weak: Weak<Inner> = Arc::downgrade(&inner);
        let reaper = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else { break };
                if inner.stopped.load(Ordering::SeqCst) {
                    break;
                }
                inner.reap().await;
            }
        });
        inner
            .tasks
            .lock()
            .expect("tasks")
            .extend([bridge_task, reaper]);
        Self { inner }
    }

    /// The signalling URL a browser connects to with its ticket.
    pub fn signal_url(&self) -> &str {
        &self.inner.config.signal_url
    }

    /// Make a pipeline available under `name`. Registering a name again
    /// replaces its manifest for sessions started afterwards.
    pub fn register_pipeline(&self, name: impl Into<String>, manifest: Arc<Manifest>) {
        self.inner
            .pipelines
            .lock()
            .expect("pipelines")
            .insert(name.into(), manifest);
    }

    /// Session lifecycle notifications from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<AgentSessionEvent> {
        self.inner.events.subscribe()
    }

    /// Admit one member: start the session of `(pipeline, session key)` if
    /// it is not running, and return a single-use ticket for the member.
    pub async fn mint_ticket(
        &self,
        request: AgentTicketRequest,
    ) -> Result<AgentTicket, AgentHostError> {
        let inner = &self.inner;
        inner.check_running()?;
        if request.session_key.is_empty() {
            return Err(AgentHostError::Invalid(
                "the session key is empty".to_string(),
            ));
        }
        let admission = Admission {
            room: agent_tickets::room_key(&request.pipeline, &request.session_key),
            member: request.member.id.clone(),
            name: request.member.name.clone().filter(|name| !name.is_empty()),
            metadata: request.member.metadata.clone(),
            grants: request.grants.clone(),
        };
        agent_tickets::validate(&admission, request.ttl)
            .map_err(|e| AgentHostError::Invalid(e.to_string()))?;

        let _guard = inner.lifecycle.lock().await;
        inner.check_running()?;
        let session_id = inner
            .ensure_session(&request.pipeline, &request.session_key)
            .await?;
        let ticket = new_ticket();
        let expires = inner
            .tickets
            .store
            .issue(ticket.clone(), admission, SystemTime::now(), request.ttl)
            .map_err(|e| AgentHostError::Invalid(e.to_string()))?;
        Ok(AgentTicket {
            ticket,
            session_id,
            expires,
        })
    }

    /// The live sessions of this host.
    pub fn sessions(&self) -> Vec<AgentSessionInfo> {
        self.inner
            .sessions
            .lock()
            .expect("sessions")
            .values()
            .map(|session| session.info.clone())
            .collect()
    }

    /// The live session with runtime id `session_id`. Waits for a session
    /// start or end in progress, so a session this host is creating is
    /// reported with its key as soon as its id exists.
    pub async fn session(&self, session_id: &str) -> Option<AgentSessionInfo> {
        let _guard = self.inner.lifecycle.lock().await;
        self.inner
            .sessions
            .lock()
            .expect("sessions")
            .values()
            .find(|session| session.info.session_id == session_id)
            .map(|session| session.info.clone())
    }

    /// The connected members of a session, as they were admitted. `None`
    /// when the session is not a live session of this host.
    pub fn members(&self, session_id: &str) -> Option<Vec<AgentMember>> {
        self.inner
            .sessions
            .lock()
            .expect("sessions")
            .values()
            .find(|session| session.info.session_id == session_id)
            .map(|session| session.members.values().cloned().collect())
    }

    /// Remove a member: its connection is dropped (it leaves with reason
    /// `removed`) and its unused tickets for the session are forgotten.
    /// `false` when it was not connected to that session.
    pub async fn remove_member(&self, session_id: &str, member_id: &str) -> bool {
        let inner = &self.inner;
        let Some(room_key) = inner.room_of_session(session_id) else {
            return false;
        };
        inner.tickets.store.revoke_member(&room_key, member_id);
        let connected = inner
            .sessions
            .lock()
            .expect("sessions")
            .get(&room_key)
            .map(|session| session.members.contains_key(member_id))
            .unwrap_or(false);
        if !connected {
            return false;
        }
        inner
            .removing
            .lock()
            .expect("removing members")
            .insert(member_id.to_string(), "removed".to_string());
        inner.drop_connection(member_id).await;
        true
    }

    /// End a session: members are disconnected, unused tickets are
    /// forgotten and the pipeline session is closed. `false` when it is not
    /// live. `reason` (default `ended`) is reported to watchers.
    pub async fn end_session(&self, session_id: &str, reason: Option<&str>) -> bool {
        let inner = &self.inner;
        let _guard = inner.lifecycle.lock().await;
        let Some(room_key) = inner.room_of_session(session_id) else {
            return false;
        };
        inner
            .end_session(
                &room_key,
                reason.filter(|r| !r.is_empty()).unwrap_or("ended"),
            )
            .await
    }

    /// End every session (reason `shutdown`) and stop the host's tasks. The
    /// signalling listener is the caller's to stop.
    pub async fn shutdown(&self) {
        let inner = &self.inner;
        if inner.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let _guard = inner.lifecycle.lock().await;
            let rooms: Vec<String> = inner
                .sessions
                .lock()
                .expect("sessions")
                .keys()
                .cloned()
                .collect();
            for room_key in rooms {
                inner.end_session(&room_key, "shutdown").await;
            }
        }
        let tasks: Vec<_> = inner.tasks.lock().expect("tasks").drain(..).collect();
        for task in tasks {
            task.abort();
        }
    }
}
