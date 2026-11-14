//! The seam between a connection and the session its member joined.
//!
//! [`crate::room::RoomJoiner`] is the real one: it joins the member to its
//! room's shared pipeline session. Tests put a stub in its place, which is
//! also how the transport is exercised before `RealtimeEventsNode` exists.

use async_trait::async_trait;
use remotemedia_core::transport::{turn_audio::FlushTarget, TransportData};
use remotemedia_webrtc::signaling::websocket::{AdmittedPeer, SharedState};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Something the session produced that may concern this connection. The
/// connection decides: it drops what is not its member's.
#[derive(Debug, Clone)]
pub enum Outbound {
    /// An audio output of the session, with its transport metadata
    /// (`participant.*`, the turn id, `rm.item_id`).
    Audio(TransportData),
    /// A frame a `RealtimeEventsNode` instance wrote to its `realtime` port,
    /// and the participant the router attributed it to.
    Event {
        participant: Option<String>,
        frame: Value,
    },
    /// An audio flush asked of the session: `None` is the session-wide one,
    /// `Some` a targeted one (design D11).
    Flush(Option<FlushTarget>),
}

/// The member's way into the session.
#[async_trait]
pub trait MemberInput: Send + Sync {
    /// The member's audio: mono `f32` at `sample_rate` (the pipeline rate).
    async fn audio(&self, samples: Vec<f32>, sample_rate: u32) -> Result<(), String>;
    /// A frame for the `realtime` port of the member's node: a client event,
    /// or one of the transport's internal frames.
    async fn event(&self, frame: Value) -> Result<(), String>;
    /// The connection ended: the member leaves the session.
    async fn leave(&self);
}

/// A member joined to its session.
pub struct MemberLink {
    pub input: Arc<dyn MemberInput>,
    /// Ends when the session does.
    pub outputs: mpsc::Receiver<Outbound>,
    /// `lead_ms` of the session's `RealtimeEventsNode`, when it sets one.
    pub lead_ms: Option<u64>,
    /// The rate the session's pipeline takes member audio at, when the
    /// session says so (`input_rate` of its `RealtimeEventsNode`).
    pub pipeline_sample_rate: Option<u32>,
}

/// Joins an admitted member to its session.
#[async_trait]
pub trait MemberJoiner: Send + Sync {
    /// `connection` identifies this connection of the member (a member that
    /// reconnects gets another).
    async fn join(
        &self,
        state: &Arc<SharedState>,
        member: &AdmittedPeer,
        connection: &str,
    ) -> Result<MemberLink, String>;
}
