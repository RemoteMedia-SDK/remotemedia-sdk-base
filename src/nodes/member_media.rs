//! `MemberMediaRelayNode` and `MemberFlushNode`: a turn node's link to the
//! members' media when it runs somewhere else than their connections
//! (openspec: add-ai-sdk-voice-library, design D12).
//!
//! A session's media events (`media_ready`, `media_lost`, `audio_released`,
//! the `__media__` topic), its targeted audio flush and a `barge_in` publish
//! all belong to the session control of the process that holds the members'
//! connections. When a deployment splits a pipeline over several processes,
//! each part has its own session control, and only the pipeline's connections
//! cross between them. A `HostedTurnNode` placed away from the members would
//! then not know what they heard, and could not cut their audio.
//!
//! These two nodes carry both over the pipeline's connections:
//!
//! ```text
//! media_relay ──────────────► turn ──(flush)──► media_flush
//! (source: with the members)        (sink: with the members)
//! ```
//!
//! - [`MemberMediaRelayNode`] has no input connection, so a deployment places
//!   it with the members. It emits every media event of its session control
//!   as a Json frame `{ "v": 1, "member_media": <event> }`, the members that
//!   are ready when it starts, and the ready members again every
//!   `resend_ms`, so a turn node that started later (or moved) learns them.
//! - [`MemberFlushNode`] has no output connection, so it is placed with the
//!   members too. It reads the turn node's `flush` output
//!   (`{ "kind": "flush_audio", members, turn_id, drop_rest, barge }`), asks
//!   its own session control for the targeted flush and, for a cancelled
//!   turn, publishes `barge_in` to the speech nodes named in its parameters.
//!
//! In one process both are redundant and harmless: the turn node uses what
//! its own session control tells it and ignores the relayed frames, and
//! emits nothing on `flush`.
//!
//! # Who may send a relayed event
//!
//! The relay is an entry point of the pipeline, like the node that takes the
//! members' typed frames, so a browser's data-channel frame can reach the
//! turn node on a parallel path. A relayed frame therefore counts only with
//! the origin field [`RELAY_ORIGIN_FIELD`], which the relay sets and a
//! member's input never carries (a member's `participant.*` fields are set by
//! the transport from its admission).

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, NodeRuntimeContext, StreamingNode, StreamingNodeFactory,
};
use crate::transport::data::participant;
use crate::transport::origin::Origin;
use crate::transport::session_control::{ControlAddress, BARGE_IN_PORT};
use crate::transport::turn_audio::{FlushTarget, MediaEvent};
use remotemedia_traits::runtime_context::NodeRuntimeContextRead;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

/// The relay's node type name in manifests.
pub const MEMBER_MEDIA_RELAY_NODE: &str = "MemberMediaRelayNode";
/// The flush sink's node type name in manifests.
pub const MEMBER_FLUSH_NODE: &str = "MemberFlushNode";
/// Key of a relayed media event: `{ "v": 1, "member_media": <event> }`.
pub const RELAY_FRAME_KEY: &str = "member_media";
/// Origin field of a relayed frame. A member's input never carries it.
pub const RELAY_ORIGIN_FIELD: &str = "participant.relay";
/// Value of [`RELAY_ORIGIN_FIELD`].
pub const RELAY_ORIGIN_VALUE: &str = "member_media";
/// `kind` of a flush request on the turn node's `flush` output.
pub const FLUSH_FRAME_KIND: &str = "flush_audio";
/// `transport` of a `media_ready` the relay sends for a member that was
/// ready before it looked: the connection's transport is not known then.
pub const RELAYED_TRANSPORT: &str = "relayed";

/// The frame the relay emits for `event`.
pub fn relay_frame(event: &MediaEvent) -> RuntimeData {
    RuntimeData::Json(json!({
        "v": 1,
        RELAY_FRAME_KEY: serde_json::to_value(event).unwrap_or_default(),
    }))
}

/// The origin of a relayed frame about `member`: the member, marked as the
/// relay's word and not the member's own input.
pub fn relay_origin(member: &str) -> Option<Arc<Origin>> {
    let fields = HashMap::from([
        (participant::ID.to_string(), member.to_string()),
        (
            participant::ROLE.to_string(),
            participant::role::SYSTEM.to_string(),
        ),
        (
            RELAY_ORIGIN_FIELD.to_string(),
            RELAY_ORIGIN_VALUE.to_string(),
        ),
    ]);
    Origin::from_metadata(&fields).map(Arc::new)
}

/// The media event of a relayed frame. `None` for anything else, and for a
/// frame of that shape that did not come from a relay (see the module docs).
pub fn relayed_event(value: &Value, origin: Option<&Origin>) -> Option<MediaEvent> {
    if value.get("v").and_then(Value::as_u64) != Some(1) {
        return None;
    }
    let event = value.get(RELAY_FRAME_KEY)?;
    let relayed = origin
        .and_then(|origin| origin.fields().get(RELAY_ORIGIN_FIELD))
        .is_some_and(|value| value == RELAY_ORIGIN_VALUE);
    if !relayed {
        tracing::debug!("a member_media frame that is not a relay's was ignored");
        return None;
    }
    serde_json::from_value(event.clone()).ok()
}

/// Whether a Json value has the shape of a relayed frame, whoever sent it.
pub fn is_relay_frame(value: &Value) -> bool {
    value.get("v").and_then(Value::as_u64) == Some(1) && value.get(RELAY_FRAME_KEY).is_some()
}

/// A flush the turn node asks of the process that holds the members'
/// connections: the payload of its `flush` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushRequest {
    /// Always [`FLUSH_FRAME_KIND`].
    pub kind: String,
    /// See [`FlushTarget::members`].
    #[serde(default)]
    pub members: Vec<String>,
    pub turn_id: String,
    #[serde(default)]
    pub drop_rest: bool,
    /// The turn was cancelled for its whole audience: the speech nodes stop
    /// what they are synthesizing.
    #[serde(default)]
    pub barge: bool,
}

impl FlushRequest {
    pub fn new(target: &FlushTarget, barge: bool) -> Self {
        Self {
            kind: FLUSH_FRAME_KIND.to_string(),
            members: target.members.clone(),
            turn_id: target.turn_id.clone(),
            drop_rest: target.drop_rest,
            barge,
        }
    }

    /// The request in a frame; `None` for anything else.
    pub fn from_runtime(data: &RuntimeData) -> Option<Self> {
        let request: Self = match data {
            RuntimeData::Json(value) => serde_json::from_value(value.clone()).ok()?,
            RuntimeData::Text(text) => serde_json::from_str(text.trim()).ok()?,
            _ => return None,
        };
        (request.kind == FLUSH_FRAME_KIND && !request.turn_id.is_empty()).then_some(request)
    }

    pub fn target(&self) -> FlushTarget {
        FlushTarget {
            members: self.members.clone(),
            turn_id: self.turn_id.clone(),
            drop_rest: self.drop_rest,
        }
    }
}

// ---------------------------------------------------------------------------
// MemberMediaRelayNode
// ---------------------------------------------------------------------------

fn default_resend_ms() -> u64 {
    2000
}

/// `MemberMediaRelayNode` parameters. None is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberMediaRelayConfig {
    /// How often the members that are media-ready are sent again, for a turn
    /// node that started after their `media_ready`. 0: never.
    #[serde(default = "default_resend_ms", alias = "resendMs")]
    pub resend_ms: u64,
}

impl Default for MemberMediaRelayConfig {
    fn default() -> Self {
        Self {
            resend_ms: default_resend_ms(),
        }
    }
}

/// See the module docs.
pub struct MemberMediaRelayNode {
    node_id: String,
    config: MemberMediaRelayConfig,
}

impl MemberMediaRelayNode {
    pub fn new(node_id: impl Into<String>, config: MemberMediaRelayConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
        }
    }
}

fn event_member(event: &MediaEvent) -> &str {
    match event {
        MediaEvent::MediaReady { member, .. } | MediaEvent::MediaLost { member, .. } => member,
        MediaEvent::AudioReleased(report) => &report.member,
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for MemberMediaRelayNode {
    fn node_type(&self) -> &str {
        MEMBER_MEDIA_RELAY_NODE
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "MemberMediaRelayNode runs in a streaming session: it emits from its background body"
                .into(),
        ))
    }

    /// The node is an entry point, so the members' input reaches it. It is
    /// not what the node relays.
    async fn process_streaming_with_ctx<F>(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
        _callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        Ok(0)
    }

    fn has_background(&self) -> bool {
        true
    }

    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        let Some(ctx) = NodeRuntimeContext::of(ctx) else {
            return Err(Error::Execution(
                "MemberMediaRelayNode needs the host runtime's context".into(),
            ));
        };
        let control = ctx.control.clone();
        let emitter = ctx.emitter();
        // Subscribed before the snapshot: an event in between is sent twice,
        // not lost, and the turn node takes a repeated `media_ready` once.
        let mut media = Some(control.subscribe_media());
        let ready = |member: String| MediaEvent::MediaReady {
            member,
            transport: RELAYED_TRANSPORT.to_string(),
            ts_ms: crate::transport::turn_audio::now_ms(),
        };
        let mut pending: Vec<MediaEvent> = control
            .media_ready_members()
            .into_iter()
            .map(ready)
            .collect();
        let resend =
            (self.config.resend_ms > 0).then(|| Duration::from_millis(self.config.resend_ms));
        let mut next_resend = resend.map(|every| tokio::time::Instant::now() + every);
        loop {
            for event in pending.drain(..) {
                let origin = relay_origin(event_member(&event));
                if let Err(error) = emitter.emit_as(relay_frame(&event), origin).await {
                    // The instance stopped.
                    tracing::debug!(node = %self.node_id, %error, "media relay stops");
                    return Ok(());
                }
            }
            tokio::select! {
                data = recv_media(&mut media) => {
                    if let Some(event) = MediaEvent::from_runtime(&data) {
                        pending.push(event);
                    }
                }
                _ = sleep_until(next_resend) => {
                    next_resend = resend.map(|every| tokio::time::Instant::now() + every);
                    pending.extend(control.media_ready_members().into_iter().map(ready));
                }
            }
        }
    }
}

async fn recv_media(rx: &mut Option<broadcast::Receiver<RuntimeData>>) -> RuntimeData {
    loop {
        let Some(receiver) = rx else {
            return std::future::pending().await;
        };
        match receiver.recv().await {
            Ok(data) => return data,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                tracing::warn!(missed, "MemberMediaRelayNode fell behind on media events");
            }
            Err(broadcast::error::RecvError::Closed) => *rx = None,
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Factory for [`MemberMediaRelayNode`].
pub struct MemberMediaRelayNodeFactory;

impl StreamingNodeFactory for MemberMediaRelayNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() {
            json!({})
        } else {
            params.clone()
        };
        let config: MemberMediaRelayConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!(
                "{MEMBER_MEDIA_RELAY_NODE} '{node_id}': invalid params: {error}"
            ))
        })?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(
            MemberMediaRelayNode::new(node_id, config),
        ))))
    }

    fn node_type(&self) -> &str {
        MEMBER_MEDIA_RELAY_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(MEMBER_MEDIA_RELAY_NODE)
                .description(
                    "Emits the session's member media events (`media_ready`, `media_lost`, \
                     `audio_released`) as Json frames `{ v: 1, member_media: <event> }`, for a \
                     `HostedTurnNode` that may run in another process than the members' \
                     connections. A source: connect its output to the turn node. The members' \
                     input that reaches it is dropped.",
                )
                .category("conversation")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                ])
                .produces([RuntimeDataType::Json])
                .config_schema_from::<MemberMediaRelayConfig>(),
        )
    }
}

// ---------------------------------------------------------------------------
// MemberFlushNode
// ---------------------------------------------------------------------------

/// `MemberFlushNode` parameters. None is required.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MemberFlushConfig {
    /// The nodes that synthesize the turn node's speech (the first node of
    /// the speech chain): each gets a `barge_in` when a turn is cancelled.
    #[serde(default, alias = "speechNodes")]
    pub speech_nodes: Vec<String>,
}

/// See the module docs.
pub struct MemberFlushNode {
    node_id: String,
    config: MemberFlushConfig,
}

impl MemberFlushNode {
    pub fn new(node_id: impl Into<String>, config: MemberFlushConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
        }
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for MemberFlushNode {
    fn node_type(&self) -> &str {
        MEMBER_FLUSH_NODE
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "MemberFlushNode runs in a streaming session".into(),
        ))
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        _callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let Some(request) = FlushRequest::from_runtime(&data) else {
            return Ok(0);
        };
        let Some(ctx) = NodeRuntimeContext::of(ctx) else {
            return Ok(0);
        };
        // The speech nodes stop first: the sender's report of the flush then
        // tells the turn node that both happened.
        if request.barge {
            // The turn node dropped the turn: its queued speech goes with it
            // (openspec: add-generator-cancellation).
            let payload = crate::transport::session_control::barge_in_payload(Some(
                remotemedia_traits::DispositionOverride {
                    pending_input: Some(remotemedia_traits::Retention::Discard),
                    ..Default::default()
                },
            ));
            for node in &self.config.speech_nodes {
                let address = ControlAddress::node_in(node).with_port(BARGE_IN_PORT);
                if let Err(error) = ctx.control.publish(&address, payload.clone()).await {
                    tracing::debug!(node = %self.node_id, target = %node, %error, "barge not delivered");
                }
            }
        }
        let accepted = ctx.control.request_flush_audio_for(request.target());
        tracing::debug!(
            node = %self.node_id, turn = %request.turn_id, members = ?request.members, accepted,
            "flush asked by the turn node"
        );
        // A sink's output goes to the members: the node has none.
        Ok(0)
    }
}

/// Factory for [`MemberFlushNode`].
pub struct MemberFlushNodeFactory;

impl StreamingNodeFactory for MemberFlushNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() {
            json!({})
        } else {
            params.clone()
        };
        let config: MemberFlushConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!(
                "{MEMBER_FLUSH_NODE} '{node_id}': invalid params: {error}"
            ))
        })?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(MemberFlushNode::new(
            node_id, config,
        )))))
    }

    fn node_type(&self) -> &str {
        MEMBER_FLUSH_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(MEMBER_FLUSH_NODE)
                .description(
                    "Carries out the audio flushes a `HostedTurnNode` asks for on its `flush` \
                     output, in the process that holds the members' connections: the targeted \
                     flush of a turn's queued audio and, for a cancelled turn, `barge_in` to the \
                     speech nodes in `speech_nodes`. A sink that emits nothing.",
                )
                .category("conversation")
                .accepts([RuntimeDataType::Json])
                .config_schema_from::<MemberFlushConfig>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::turn_audio::AudioReleased;

    fn released() -> MediaEvent {
        MediaEvent::AudioReleased(AudioReleased {
            member: "ada".into(),
            turn_id: "turn-1".into(),
            released_ms: 1400,
            chunks_released: 1,
            chunk: 1,
            flushed: false,
            drained: false,
            stream_id: None,
        })
    }

    #[test]
    fn a_relayed_event_round_trips_and_needs_the_relays_origin() {
        let event = released();
        let RuntimeData::Json(frame) = relay_frame(&event) else {
            panic!("a Json frame")
        };
        assert_eq!(frame["v"], 1);
        assert_eq!(frame[RELAY_FRAME_KEY]["kind"], "audio_released");
        assert!(is_relay_frame(&frame));
        let origin = relay_origin("ada").unwrap();
        assert_eq!(origin.id(), "ada");
        assert_eq!(relayed_event(&frame, Some(&origin)), Some(event));
        // The same frame as a member's own input, or as nobody's.
        let member = Origin::from_metadata(&HashMap::from([
            (participant::ID.to_string(), "ada".to_string()),
            (
                participant::ROLE.to_string(),
                participant::role::CLIENT.to_string(),
            ),
        ]))
        .unwrap();
        assert_eq!(relayed_event(&frame, Some(&member)), None);
        assert_eq!(relayed_event(&frame, None), None);
        // Other frames are not relayed events.
        assert!(!is_relay_frame(
            &json!({ "v": 1, "type": "text", "text": "hi" })
        ));
        assert_eq!(
            relayed_event(&json!({ "v": 2, RELAY_FRAME_KEY: {} }), Some(&origin)),
            None
        );
    }

    #[test]
    fn a_flush_request_carries_the_target_and_the_barge() {
        let target = FlushTarget {
            members: vec!["ada".into()],
            turn_id: "turn-1".into(),
            drop_rest: true,
        };
        let request = FlushRequest::new(&target, true);
        let frame = RuntimeData::Json(serde_json::to_value(&request).unwrap());
        assert_eq!(
            frame,
            RuntimeData::Json(json!({
                "kind": "flush_audio", "members": ["ada"], "turn_id": "turn-1", "drop_rest": true, "barge": true,
            }))
        );
        let read = FlushRequest::from_runtime(&frame).unwrap();
        assert_eq!((read.target(), read.barge), (target, true));
        // As text, and with the defaults.
        let text = RuntimeData::Text(r#"{"kind":"flush_audio","turn_id":"t"}"#.into());
        let read = FlushRequest::from_runtime(&text).unwrap();
        assert_eq!(
            (read.members.len(), read.drop_rest, read.barge),
            (0, false, false)
        );
        for other in [
            RuntimeData::Json(json!({ "kind": "media_ready", "member": "ada", "turn_id": "t" })),
            RuntimeData::Json(json!({ "kind": "flush_audio", "turn_id": "" })),
            RuntimeData::Text("barge".into()),
        ] {
            assert!(FlushRequest::from_runtime(&other).is_none(), "{other:?}");
        }
    }

    #[test]
    fn the_factories_take_no_parameters_and_reject_wrong_ones() {
        assert!(MemberMediaRelayNodeFactory
            .create("relay".into(), &Value::Null, None)
            .is_ok());
        assert!(MemberFlushNodeFactory
            .create("flush".into(), &Value::Null, None)
            .is_ok());
        assert!(MemberFlushNodeFactory
            .create("flush".into(), &json!({ "speech_nodes": ["tts"] }), None)
            .is_ok());
        assert!(MemberFlushNodeFactory
            .create("flush".into(), &json!({ "speech_nodes": "tts" }), None)
            .is_err());
        assert!(MemberMediaRelayNodeFactory
            .create("relay".into(), &json!({ "resend_ms": "soon" }), None)
            .is_err());
    }
}
