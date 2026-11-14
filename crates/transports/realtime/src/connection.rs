//! One realtime connection: the socket of one member (design D10, "Runtime
//! side: a thin transport and a pipeline node").
//!
//! The connection owns the socket and the clock. It converts audio, paces
//! what it sends, applies the session-configuration policy and drops what is
//! not its member's. Every other frame is passed, unchanged, between the
//! socket and the `realtime` port of the member's `RealtimeEventsNode`.

use crate::admission::RealtimeAdmission;
use crate::audio::{decode_pcm16_base64, encode_pcm16_base64, to_mono, to_pcm16, Resampler};
use crate::link::{MemberLink, Outbound};
use crate::pacer::{Pacer, Released};
use crate::protocol;
use crate::RealtimeConfig;
use futures_util::{SinkExt, StreamExt};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::data::participant;
use remotemedia_core::transport::turn_audio::{FlushTarget, TurnTag};
use remotemedia_core::transport::TransportData;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, info, warn};

/// Frames waiting for the pipeline. Audio beyond it is dropped; events wait.
const INPUT_QUEUE: usize = 1024;

/// Close code of a connection replaced by a newer one of the same member.
pub const CLOSE_REPLACED: u16 = 4000;
/// Close code when the member could not join its session.
pub const CLOSE_JOIN_FAILED: u16 = 4001;

/// What goes to the pipeline, in the order the socket produced it.
enum Input {
    Audio(Vec<f32>, u32),
    Event(Value),
}

struct Connection {
    member: String,
    admission: RealtimeAdmission,
    pipeline_rate: u32,
    input_rate: u32,
    output_rate: u32,
    /// `outputModalities` includes audio.
    audio_output: bool,
    input_resampler: Resampler,
    /// The resampler of the item being received, by item id and source rate.
    output_resampler: Option<(String, Resampler)>,
    pacer: Pacer,
    ignored_reported: bool,
    untagged_warned: bool,
    to_pipeline: mpsc::Sender<Input>,
}

/// Send a close frame and finish the closing handshake as far as the peer
/// cooperates.
async fn close<S>(socket: &mut WebSocketStream<S>, code: u16, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: reason.to_string().into(),
        })))
        .await;
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        while let Some(Ok(_)) = socket.next().await {}
    })
    .await;
}

/// Close a socket whose member could not join its session.
pub async fn refuse<S>(mut socket: WebSocketStream<S>, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    close(&mut socket, CLOSE_JOIN_FAILED, reason).await;
}

/// Serve `socket` for `member` until either side ends it. `replaced` is
/// notified when a newer connection of the same member takes over.
pub async fn run<S>(
    mut socket: WebSocketStream<S>,
    member: String,
    admission: RealtimeAdmission,
    link: MemberLink,
    config: RealtimeConfig,
    connection_id: String,
    replaced: Arc<Notify>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let MemberLink {
        input,
        mut outputs,
        lead_ms,
        pipeline_sample_rate,
    } = link;
    let pipeline_rate = pipeline_sample_rate.unwrap_or(config.pipeline_sample_rate).max(1);
    let lead = Duration::from_millis(lead_ms.unwrap_or(config.lead_ms));

    // The pipeline is fed from its own task: a full router queue delays
    // input, never what is sent to the browser.
    let (to_pipeline, mut from_socket) = mpsc::channel::<Input>(INPUT_QUEUE);
    let feeder_input = Arc::clone(&input);
    let feeder_member = member.clone();
    let feeder = tokio::spawn(async move {
        while let Some(next) = from_socket.recv().await {
            let sent = match next {
                Input::Audio(samples, rate) => feeder_input.audio(samples, rate).await,
                Input::Event(frame) => feeder_input.event(frame).await,
            };
            if let Err(error) = sent {
                debug!("realtime input of {} ended: {}", feeder_member, error);
                break;
            }
        }
    });

    let mut connection = Connection {
        member: member.clone(),
        admission,
        pipeline_rate,
        input_rate: config.default_input_rate,
        output_rate: config.default_output_rate,
        audio_output: true,
        input_resampler: Resampler::new(config.default_input_rate, pipeline_rate),
        output_resampler: None,
        pacer: Pacer::new(
            lead,
            Duration::from_millis(config.send_interval_ms.max(1)),
            Duration::from_millis(config.release_report_ms.max(1)),
        ),
        ignored_reported: false,
        untagged_warned: false,
        to_pipeline,
    };

    info!(
        "realtime connection {} of member {}: lead {} ms, pipeline rate {} Hz",
        connection_id,
        member,
        lead.as_millis(),
        pipeline_rate
    );

    // The node learns the member is here, then the browser that it is ready.
    connection
        .to_node(protocol::joined(
            &connection.member,
            connection.output_rate,
            connection.input_rate,
        ))
        .await;
    let created = json!({ "type": protocol::SESSION_CREATED, "sessionId": connection_id });
    let mut close_with: Option<(u16, &str)> = None;
    if socket.send(Message::Text(created.to_string())).await.is_ok() {
        loop {
            let wake = connection.pacer.next_wake(Instant::now());
            let timer = async {
                match wake {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                message = socket.next() => {
                    let text = match message {
                        Some(Ok(Message::Text(text))) => text,
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => continue,
                        Some(Err(error)) => {
                            debug!("realtime socket of {} failed: {}", member, error);
                            break;
                        }
                    };
                    let replies = connection.on_client_frame(&text).await;
                    if !write_all(&mut socket, replies).await {
                        break;
                    }
                }
                output = outputs.recv() => {
                    let Some(output) = output else {
                        close_with = Some((1001, "the session ended"));
                        break;
                    };
                    let frames = connection.on_outbound(output).await;
                    if !write_all(&mut socket, frames).await {
                        break;
                    }
                }
                _ = timer => {}
                _ = replaced.notified() => {
                    close_with = Some((CLOSE_REPLACED, "replaced by a newer connection"));
                    break;
                }
            }
            let frames = connection.pump().await;
            if !write_all(&mut socket, frames).await {
                break;
            }
        }
    }

    if let Some((code, reason)) = close_with {
        close(&mut socket, code, reason).await;
    }
    drop(connection);
    // Frames already accepted from the socket still reach the pipeline
    // before the member leaves.
    let _ = tokio::time::timeout(Duration::from_secs(2), feeder).await;
    input.leave().await;
    info!("realtime connection {} of member {} ended", connection_id, member);
}

async fn write_all<S>(socket: &mut WebSocketStream<S>, frames: Vec<Value>) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for frame in frames {
        debug_assert!(
            !protocol::frame_type(&frame).is_some_and(protocol::is_internal),
            "an internal frame was about to reach the socket"
        );
        if socket.send(Message::Text(frame.to_string())).await.is_err() {
            return false;
        }
    }
    true
}

impl Connection {
    /// A frame for the member's node. Waits when the pipeline is behind.
    async fn to_node(&self, frame: Value) {
        let _ = self.to_pipeline.send(Input::Event(frame)).await;
    }

    async fn report(&self, reports: Vec<Released>) {
        for report in reports {
            self.to_node(json!({
                "type": protocol::INTERNAL_RELEASED,
                "itemId": report.item_id,
                "audioMs": report.audio_ms,
                "responseId": report.response_id,
                "receivedMs": report.received_ms,
                "flushed": report.flushed,
                "drained": report.drained,
            }))
            .await;
        }
    }

    /// Write what the pacer allows now; report what was released.
    async fn pump(&mut self) -> Vec<Value> {
        let (deltas, reports) = self.pacer.poll(Instant::now());
        self.report(reports).await;
        if !self.audio_output {
            // Without the audio modality the audio is accounted for at the
            // same pace, so the node's transcript still follows it.
            return Vec::new();
        }
        deltas
            .into_iter()
            .map(|delta| {
                protocol::audio_delta(
                    &delta.response_id,
                    &delta.item_id,
                    encode_pcm16_base64(&delta.samples),
                )
            })
            .collect()
    }

    /// A frame from the browser. Returns what to answer on the socket.
    async fn on_client_frame(&mut self, text: &str) -> Vec<Value> {
        let mut frame: Value = match serde_json::from_str(text) {
            Ok(frame) => frame,
            Err(error) => {
                debug!("realtime frame of {} is not JSON: {}", self.member, error);
                return Vec::new();
            }
        };
        let Some(kind) = protocol::frame_type(&frame).map(str::to_string) else {
            debug!("realtime frame of {} has no type", self.member);
            return Vec::new();
        };
        match kind.as_str() {
            protocol::INPUT_AUDIO_APPEND => {
                self.on_input_audio(&frame);
                Vec::new()
            }
            protocol::SESSION_UPDATE => self.on_session_update(&frame).await,
            // Internal frames are the transport's and the node's. A browser
            // cannot send one.
            kind if protocol::is_internal(kind) => {
                warn!("member {} sent the internal frame {}; dropped", self.member, kind);
                Vec::new()
            }
            _ => {
                // `corr_id` is the control bus's RPC marker, never a field of
                // a realtime event.
                if let Some(object) = frame.as_object_mut() {
                    object.remove("corr_id");
                }
                self.to_node(frame).await;
                Vec::new()
            }
        }
    }

    fn on_input_audio(&mut self, frame: &Value) {
        let Some(audio) = frame.get("audio").and_then(Value::as_str) else {
            return;
        };
        let samples = match decode_pcm16_base64(audio) {
            Ok(samples) => samples,
            Err(error) => {
                debug!("input audio of {} dropped: {}", self.member, error);
                return;
            }
        };
        let samples = self.input_resampler.process(&samples);
        if samples.is_empty() {
            return;
        }
        // Audio is never worth stalling the socket for.
        if self
            .to_pipeline
            .try_send(Input::Audio(samples, self.pipeline_rate))
            .is_err()
        {
            warn!("pipeline input of {} is full; audio dropped", self.member);
        }
    }

    /// The session-configuration policy (design D10): the formats and
    /// modalities are applied here, the fields the member's admission allows
    /// go to the node, the rest is ignored and named once per connection.
    /// The answer is always `session-updated`.
    async fn on_session_update(&mut self, frame: &Value) -> Vec<Value> {
        let filtered = self
            .admission
            .filter(frame.get("config").unwrap_or(&Value::Null));
        if let Some(rate) = filtered.input_rate {
            if rate != self.input_rate {
                self.input_rate = rate;
                self.input_resampler = Resampler::new(rate, self.pipeline_rate);
            }
        }
        let mut rates_changed = false;
        if let Some(rate) = filtered.output_rate {
            if rate != self.output_rate {
                self.output_rate = rate;
                self.output_resampler = None;
                rates_changed = true;
            }
        }
        if let Some(audio_output) = filtered.audio_output {
            self.audio_output = audio_output;
        }
        if rates_changed {
            // The node counts `audioEndMs` at the rate the browser plays at.
            self.to_node(protocol::joined(&self.member, self.output_rate, self.input_rate))
                .await;
        }
        if !filtered.forward.is_empty() {
            self.to_node(json!({
                "type": protocol::SESSION_UPDATE,
                "config": Value::Object(filtered.forward),
            }))
            .await;
        }

        let mut replies = Vec::new();
        if !filtered.ignored.is_empty() && !self.ignored_reported {
            self.ignored_reported = true;
            replies.push(json!({
                "type": protocol::CUSTOM,
                "rawType": protocol::CONFIG_IGNORED,
                "raw": { "fields": filtered.ignored, "tools": filtered.ignored_tools },
            }));
        }
        replies.push(json!({ "type": protocol::SESSION_UPDATED }));
        replies
    }

    /// Something from the session. Returns what to write to the socket now.
    async fn on_outbound(&mut self, output: Outbound) -> Vec<Value> {
        match output {
            Outbound::Audio(audio) => {
                self.on_audio(audio);
                Vec::new()
            }
            Outbound::Event { participant, frame } => self.on_node_frame(participant, frame).await,
            Outbound::Flush(target) => {
                self.on_session_flush(target).await;
                Vec::new()
            }
        }
    }

    /// Whether an audio output is this member's. An addressed output must
    /// name the member; one attributed to a participant must be the
    /// member's; an unattributed one is for everyone. So is a turn's audio
    /// that the session's `RealtimeEventsNode` stamped with its item and that
    /// names nobody: the turn node addresses a turn for some members only
    /// (`participant.to`), and leaves one for the whole session unaddressed
    /// (design D11, "Audience").
    fn audio_is_mine(&self, audio: &TransportData) -> bool {
        if participant::recipients(&audio.metadata).is_some() {
            return participant::is_for(&audio.metadata, &self.member);
        }
        if audio.metadata.contains_key(protocol::ITEM_ID_KEY)
            && TurnTag::from_metadata(&audio.metadata).is_some()
        {
            return true;
        }
        match audio.metadata.get(participant::ID) {
            Some(id) => id == &self.member,
            None => true,
        }
    }

    fn on_audio(&mut self, audio: TransportData) {
        if !self.audio_is_mine(&audio) {
            return;
        }
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = &audio.data
        else {
            return;
        };
        // The ids the node stamped; the turn tag alone is enough.
        let turn = TurnTag::from_metadata(&audio.metadata).map(|tag| tag.turn_id.to_string());
        let item = audio
            .metadata
            .get(protocol::ITEM_ID_KEY)
            .filter(|item| !item.is_empty())
            .cloned();
        let (item_id, response_id) = match (item, turn) {
            (Some(item), Some(turn)) => (item, turn),
            (None, Some(turn)) => (format!("{turn}:assistant"), turn),
            (Some(item), None) => {
                let response = item.strip_suffix(":assistant").unwrap_or(&item).to_string();
                (item, response)
            }
            (None, None) => {
                if !std::mem::replace(&mut self.untagged_warned, true) {
                    warn!(
                        "audio for {} carries no turn or item id; it cannot be an audio-delta and is dropped",
                        self.member
                    );
                }
                return;
            }
        };
        let source_rate = (*sample_rate).max(1);
        let reuse = self.output_resampler.as_ref().is_some_and(|(current, resampler)| {
            current == &item_id
                && resampler.from_rate() == source_rate
                && resampler.to_rate() == self.output_rate
        });
        if !reuse {
            self.output_resampler =
                Some((item_id.clone(), Resampler::new(source_rate, self.output_rate)));
        }
        let mono = to_mono(samples.as_slice(), *channels);
        let resampled = match self.output_resampler.as_mut() {
            Some((_, resampler)) => resampler.process(&mono),
            None => mono,
        };
        self.pacer
            .push(&item_id, &response_id, self.output_rate, to_pcm16(&resampled));
    }

    async fn on_node_frame(&mut self, participant: Option<String>, frame: Value) -> Vec<Value> {
        // One member per connection: a frame of another member's instance,
        // or of none, is not shown here.
        if participant.as_deref() != Some(self.member.as_str()) {
            debug!(
                "realtime frame attributed to {:?} dropped on the connection of {}",
                participant, self.member
            );
            return Vec::new();
        }
        let Some(kind) = protocol::frame_type(&frame) else {
            return Vec::new();
        };
        if protocol::is_internal(kind) {
            if kind == protocol::INTERNAL_FLUSH {
                let item = frame.get("itemId").and_then(Value::as_str);
                let reports = self.pacer.flush_item(item, Instant::now());
                if let Some((current, _)) = &self.output_resampler {
                    if item.is_none_or(|item| item == current) {
                        self.output_resampler = None;
                    }
                }
                self.report(reports).await;
            }
            // Never to the socket.
            return Vec::new();
        }
        vec![frame]
    }

    /// A flush asked of the whole session or of some members (design D11).
    async fn on_session_flush(&mut self, target: Option<FlushTarget>) {
        let now = Instant::now();
        let reports = match target {
            None => self.pacer.flush_item(None, now),
            Some(target) => {
                if !target.members.is_empty() && !target.members.contains(&self.member) {
                    return;
                }
                self.pacer.flush_turn(&target.turn_id, target.drop_rest, now)
            }
        };
        self.output_resampler = None;
        self.report(reports).await;
    }
}
