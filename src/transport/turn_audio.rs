//! Turn-tagged outbound audio (openspec: add-ai-sdk-voice-library, group 2).
//!
//! A turn's text carries a turn id and a chunk index as `participant.*`
//! metadata ([`keys`]). The router keeps `participant.*` fields next to every
//! payload ([`super::origin`]), so the audio a TTS node produces for a text
//! chunk reaches the transport with the same tag and no help from the node.
//! The transport's audio sender, the last hop before the network, then knows
//! which turn and chunk each queued frame belongs to, and can:
//!
//! - report what it has released ([`AudioReleased`], from a [`ReleaseTracker`]);
//! - drop one turn's queued audio for some members only ([`FlushTarget`]).
//!
//! Member readiness ([`MediaEvent::MediaReady`]) and the reports are published
//! on the session bus topic [`MEDIA_PORT`].

use crate::data::RuntimeData;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Frame metadata keys of a turn's text and audio.
pub mod keys {
    /// The turn a frame belongs to. Unique within a session.
    pub const TURN_ID: &str = "participant.turn_id";
    /// Index of the turn's text chunk a frame renders, from 0, increasing.
    pub const CHUNK: &str = "participant.chunk";
    /// Accepted for the turn id where a node stamps audio itself
    /// (`RealtimeEventsNode`). [`TURN_ID`] wins when both are present.
    pub const TURN_ID_ALIAS: &str = "rm.turn_id";
}

/// Reserved session bus topic (`__media__.out`) for [`MediaEvent`]s.
pub const MEDIA_PORT: &str = "__media__";

/// Reserved input port of a TTS node: a publish to `<tts>.in.__speak__`
/// is a [`SpeechRequest`] (see `SessionControl::speak`).
pub const SPEAK_PORT: &str = "__speak__";

/// How often a sender reports a turn's released audio while sending it.
pub const RELEASE_REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// The turn and text chunk of a piece of audio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnTag {
    pub turn_id: Arc<str>,
    pub chunk: u64,
}

impl TurnTag {
    pub fn new(turn_id: impl AsRef<str>, chunk: u64) -> Self {
        Self {
            turn_id: Arc::from(turn_id.as_ref()),
            chunk,
        }
    }

    /// The tag in an output's transport metadata. A missing or unreadable
    /// chunk index is chunk 0.
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Option<Self> {
        let turn_id = metadata
            .get(keys::TURN_ID)
            .or_else(|| metadata.get(keys::TURN_ID_ALIAS))
            .filter(|id| !id.is_empty())?;
        let chunk = metadata
            .get(keys::CHUNK)
            .and_then(|chunk| chunk.trim().parse().ok())
            .unwrap_or(0);
        Some(Self::new(turn_id, chunk))
    }

    /// Write the tag into transport metadata.
    pub fn write_to(&self, metadata: &mut HashMap<String, String>) {
        metadata.insert(keys::TURN_ID.to_string(), self.turn_id.to_string());
        metadata.insert(keys::CHUNK.to_string(), self.chunk.to_string());
    }
}

/// Which queued audio a targeted flush drops
/// (`SessionControl::request_flush_audio_for`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushTarget {
    /// The members (`participant.id`) whose queued audio is dropped. Empty:
    /// every member of the session.
    #[serde(default)]
    pub members: Vec<String>,
    /// Only frames tagged with this turn are dropped.
    pub turn_id: String,
    /// Also refuse the turn's later frames for those members.
    #[serde(default)]
    pub drop_rest: bool,
}

/// What a member's audio sender has released of a turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioReleased {
    /// The member (`participant.id`) the audio was sent to.
    pub member: String,
    pub turn_id: String,
    /// Audio of the turn written to the network so far, in milliseconds.
    pub released_ms: u64,
    /// The turn's text chunks known to be wholly written: chunks before the
    /// one being sent, and chunks closed by an end mark (`speech_end`).
    pub chunks_released: u64,
    /// Index of the chunk the last written audio belongs to.
    pub chunk: u64,
    /// Sent once after a flush; the turn's queued audio was dropped.
    #[serde(default)]
    pub flushed: bool,
    /// The sender has nothing left to send. With `chunk` this tells a
    /// listener that the audio received so far has all been written.
    #[serde(default)]
    pub drained: bool,
    /// The outbound stream, on a transport with several per member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
}

/// An event on the session bus topic [`MEDIA_PORT`]: a JSON object tagged
/// by `kind` (`media_ready`, `media_lost`, `audio_released`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MediaEvent {
    /// The member can hear output. Once per connection of a member.
    MediaReady {
        member: String,
        /// `webrtc`, `telephony`, ...
        transport: String,
        ts_ms: u64,
    },
    /// The member's connection ended; it is ready again after a reconnect.
    MediaLost {
        member: String,
        transport: String,
        ts_ms: u64,
    },
    AudioReleased(AudioReleased),
}

impl MediaEvent {
    pub fn to_runtime(&self) -> RuntimeData {
        RuntimeData::Json(serde_json::to_value(self).unwrap_or_default())
    }

    /// The event in a frame read from the topic; `None` for anything else.
    pub fn from_runtime(data: &RuntimeData) -> Option<Self> {
        match data {
            RuntimeData::Json(value) => serde_json::from_value(value.clone()).ok(),
            RuntimeData::Text(text) => serde_json::from_str(text).ok(),
            _ => None,
        }
    }
}

/// Reserved session bus topic (`__members__.out`) for [`MemberEvent`]s.
pub const MEMBERS_PORT: &str = "__members__";

/// A member joined or left the session, as the router saw it: a JSON object
/// tagged by `kind` (`member_joined`, `member_left`) on [`MEMBERS_PORT`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MemberEvent {
    /// The member's first input, or an explicit join.
    MemberJoined {
        member: String,
        /// The member's `participant.*` fields.
        #[serde(default)]
        fields: HashMap<String, String>,
        ts_ms: u64,
    },
    MemberLeft { member: String, ts_ms: u64 },
}

impl MemberEvent {
    pub fn to_runtime(&self) -> RuntimeData {
        RuntimeData::Json(serde_json::to_value(self).unwrap_or_default())
    }

    /// The event in a frame read from the topic; `None` for anything else.
    pub fn from_runtime(data: &RuntimeData) -> Option<Self> {
        match data {
            RuntimeData::Json(value) => serde_json::from_value(value.clone()).ok(),
            RuntimeData::Text(text) => serde_json::from_str(text).ok(),
            _ => None,
        }
    }
}

/// Speech injected through the control bus (`SessionControl::speak`). It goes
/// through the TTS node and each member's audio sender tagged with a turn id,
/// so it is reported like a turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpeechRequest {
    /// The TTS node whose input receives the text.
    #[serde(default)]
    pub node: String,
    pub text: String,
    /// The members who hear it (`participant.to`). Empty: everyone.
    #[serde(default)]
    pub to: Vec<String>,
    /// The turn id to report it under; generated when absent.
    #[serde(default)]
    pub turn_id: Option<String>,
    /// Hold the text until every member in `to` (or, with `to` empty, any
    /// member) is media-ready. Default true.
    #[serde(default = "default_true")]
    pub wait_for_media: bool,
}

fn default_true() -> bool {
    true
}

/// What `SessionControl::speak` did with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeechAccepted {
    pub turn_id: String,
    /// The text waits for `media_ready`; it was not sent to TTS yet.
    pub queued: bool,
}

/// One report of a [`ReleaseTracker`]; the sender adds the member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseReport {
    pub turn_id: Arc<str>,
    pub released: Duration,
    pub chunks_released: u64,
    pub chunk: u64,
    pub flushed: bool,
    pub drained: bool,
}

impl ReleaseReport {
    pub fn for_member(&self, member: &str, stream_id: Option<&str>) -> AudioReleased {
        AudioReleased {
            member: member.to_string(),
            turn_id: self.turn_id.to_string(),
            released_ms: self.released.as_millis() as u64,
            chunks_released: self.chunks_released,
            chunk: self.chunk,
            flushed: self.flushed,
            drained: self.drained,
            stream_id: stream_id.map(str::to_string),
        }
    }
}

#[derive(Debug)]
struct Progress {
    turn_id: Arc<str>,
    released: Duration,
    chunk: u64,
    chunks_released: u64,
    started: bool,
    last_report: Option<Instant>,
    dirty: bool,
    /// The last report of the turn said the queue was drained. Cleared by
    /// the next frame written.
    drained_reported: bool,
}

/// How many recent turns a sender remembers, to answer a late flush.
const REMEMBERED_TURNS: usize = 8;

/// Accounting of what an audio sender has written, per turn. Owned by the
/// one thread or task that writes frames to the network: call
/// [`Self::frame_written`] after each write, and the other methods when the
/// queue reports them. Each call appends the reports that are due.
#[derive(Debug)]
pub struct ReleaseTracker {
    /// Most recent last. The last one is being sent while `open`.
    turns: VecDeque<Progress>,
    open: bool,
    interval: Duration,
}

impl Default for ReleaseTracker {
    fn default() -> Self {
        Self::new(RELEASE_REPORT_INTERVAL)
    }
}

impl ReleaseTracker {
    pub fn new(interval: Duration) -> Self {
        Self {
            turns: VecDeque::new(),
            open: false,
            interval,
        }
    }

    fn emit(progress: &mut Progress, now: Instant, flushed: bool, drained: bool, out: &mut Vec<ReleaseReport>) {
        progress.last_report = Some(now);
        progress.dirty = false;
        progress.drained_reported = drained;
        out.push(ReleaseReport {
            turn_id: progress.turn_id.clone(),
            released: progress.released,
            chunks_released: progress.chunks_released,
            chunk: progress.chunk,
            flushed,
            drained,
        });
    }

    /// Make `turn_id` the turn being sent, closing the one before it.
    fn select(&mut self, turn_id: &Arc<str>, now: Instant, out: &mut Vec<ReleaseReport>) -> &mut Progress {
        let current = self.open && self.turns.back().is_some_and(|p| p.turn_id == *turn_id);
        if !current {
            self.close_current(now, out);
            let known = self.turns.iter().position(|p| p.turn_id == *turn_id);
            let progress = match known.and_then(|at| self.turns.remove(at)) {
                Some(progress) => progress,
                None => Progress {
                    turn_id: turn_id.clone(),
                    released: Duration::ZERO,
                    chunk: 0,
                    chunks_released: 0,
                    started: false,
                    last_report: None,
                    dirty: false,
                    drained_reported: false,
                },
            };
            if self.turns.len() >= REMEMBERED_TURNS {
                self.turns.pop_front();
            }
            self.turns.push_back(progress);
            self.open = true;
        }
        self.turns.back_mut().expect("selected turn")
    }

    /// Audio of another turn (or untagged audio) follows: every chunk of the
    /// turn being sent has been written.
    fn close_current(&mut self, now: Instant, out: &mut Vec<ReleaseReport>) {
        if !std::mem::take(&mut self.open) {
            return;
        }
        if let Some(progress) = self.turns.back_mut() {
            let all = progress.chunk + 1;
            if progress.started && (progress.dirty || progress.chunks_released < all) {
                progress.chunks_released = progress.chunks_released.max(all);
                Self::emit(progress, now, false, false, out);
            }
        }
    }

    /// A frame was written to the network. `tag` is `None` for audio that
    /// belongs to no turn.
    pub fn frame_written(
        &mut self,
        tag: Option<&TurnTag>,
        duration: Duration,
        now: Instant,
        out: &mut Vec<ReleaseReport>,
    ) {
        let Some(tag) = tag else {
            self.close_current(now, out);
            return;
        };
        let interval = self.interval;
        let progress = self.select(&tag.turn_id, now, out);
        if !progress.started || tag.chunk > progress.chunk {
            // Audio of chunk k is leaving: every chunk before it has left.
            progress.chunks_released = progress.chunks_released.max(tag.chunk);
            progress.chunk = progress.chunk.max(tag.chunk);
            progress.started = true;
        }
        progress.released += duration;
        progress.dirty = true;
        progress.drained_reported = false;
        if progress
            .last_report
            .is_none_or(|last| now.duration_since(last) >= interval)
        {
            Self::emit(progress, now, false, false, out);
        }
    }

    /// An end mark was reached in the queue: all audio of the tagged chunk
    /// (or, untagged, of the chunk being sent) has been written.
    pub fn chunk_end(&mut self, tag: Option<&TurnTag>, now: Instant, out: &mut Vec<ReleaseReport>) {
        let progress = match tag {
            Some(tag) => {
                let progress = self.select(&tag.turn_id, now, out);
                progress.chunk = progress.chunk.max(tag.chunk);
                progress.started = true;
                progress
            }
            None if self.open => match self.turns.back_mut() {
                Some(progress) => progress,
                None => return,
            },
            None => return,
        };
        let done = tag.map_or(progress.chunk, |tag| tag.chunk) + 1;
        if progress.chunks_released < done || progress.dirty {
            progress.chunks_released = progress.chunks_released.max(done);
            Self::emit(progress, now, false, false, out);
        }
    }

    /// Queued audio was dropped: of `turn_id`, or with `None` everything.
    /// Reports the turn once, with what was written before the flush.
    pub fn flushed(&mut self, turn_id: Option<&str>, now: Instant, out: &mut Vec<ReleaseReport>) {
        match turn_id {
            Some(turn_id) => {
                match self.turns.iter().position(|p| &*p.turn_id == turn_id) {
                    Some(at) => {
                        Self::emit(&mut self.turns[at], now, true, false, out);
                        // Cut, not completed: what follows must not count
                        // its remaining chunks as released.
                        if at + 1 == self.turns.len() {
                            self.open = false;
                        }
                    }
                    None => {
                        // Flushed before any of it was written. It is
                        // remembered behind the turn being sent.
                        let mut progress = Progress {
                            turn_id: Arc::from(turn_id),
                            released: Duration::ZERO,
                            chunk: 0,
                            chunks_released: 0,
                            started: false,
                            last_report: None,
                            dirty: false,
                            drained_reported: false,
                        };
                        Self::emit(&mut progress, now, true, false, out);
                        if self.turns.len() >= REMEMBERED_TURNS {
                            self.turns.pop_front();
                        }
                        if self.turns.is_empty() {
                            self.open = false;
                        }
                        self.turns.push_front(progress);
                    }
                }
            }
            None => {
                if std::mem::take(&mut self.open) {
                    if let Some(progress) = self.turns.back_mut() {
                        Self::emit(progress, now, true, false, out);
                    }
                }
            }
        }
    }

    /// The queue ran empty. Reported once per run of frames, also when the
    /// periodic report of the last frame already carried its numbers: a
    /// listener waits for `drained` to know the turn's last chunk has left.
    pub fn drained(&mut self, now: Instant, out: &mut Vec<ReleaseReport>) {
        if self.open {
            if let Some(progress) = self
                .turns
                .back_mut()
                .filter(|p| p.dirty || (p.started && !p.drained_reported))
            {
                Self::emit(progress, now, false, true, out);
            }
        }
    }
}

/// The tags of PCM waiting in a frame assembler, in order. A sender pushes a
/// segment per input and takes one frame's worth per assembled frame, so
/// each frame knows its turn and chunk across re-framing.
#[derive(Debug, Default)]
pub struct TurnSegments {
    segments: VecDeque<(Option<TurnTag>, usize)>,
}

impl TurnSegments {
    /// `samples` more samples with `tag` were queued.
    pub fn push(&mut self, tag: Option<&TurnTag>, samples: usize) {
        if samples == 0 {
            return;
        }
        match self.segments.back_mut() {
            Some((last, count)) if last.as_ref() == tag => *count += samples,
            _ => self.segments.push_back((tag.cloned(), samples)),
        }
    }

    /// A frame of `samples` samples was assembled: its tag is that of its
    /// first sample.
    pub fn take(&mut self, mut samples: usize) -> Option<TurnTag> {
        let tag = self.segments.front().and_then(|(tag, _)| tag.clone());
        while samples > 0 {
            let Some((_, count)) = self.segments.front_mut() else {
                break;
            };
            let used = samples.min(*count);
            *count -= used;
            samples -= used;
            if *count == 0 {
                self.segments.pop_front();
            }
        }
        tag
    }

    pub fn clear(&mut self) {
        self.segments.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Whether anything waiting belongs to `turn_id`.
    pub fn has_turn(&self, turn_id: &str) -> bool {
        self.segments
            .iter()
            .any(|(tag, _)| tag.as_ref().is_some_and(|tag| &*tag.turn_id == turn_id))
    }

    /// Whether everything waiting belongs to `turn_id`.
    pub fn only_turn(&self, turn_id: &str) -> bool {
        !self.segments.is_empty()
            && self
                .segments
                .iter()
                .all(|(tag, _)| tag.as_ref().is_some_and(|tag| &*tag.turn_id == turn_id))
    }
}

/// The turns a member's output refuses (`FlushTarget::drop_rest`). Turn ids
/// are unique within a session, so a refused turn stays refused; the set is
/// bounded and forgets the oldest.
#[derive(Debug, Default)]
pub struct DroppedTurns {
    turns: VecDeque<Arc<str>>,
}

impl DroppedTurns {
    const KEPT: usize = 32;

    pub fn drop_turn(&mut self, turn_id: &str) {
        if !self.contains(turn_id) {
            if self.turns.len() >= Self::KEPT {
                self.turns.pop_front();
            }
            self.turns.push_back(Arc::from(turn_id));
        }
    }

    pub fn contains(&self, turn_id: &str) -> bool {
        self.turns.iter().any(|turn| &**turn == turn_id)
    }
}

/// Per-session media state behind `SessionControl`'s media methods.
#[derive(Default)]
pub(crate) struct MediaState {
    /// Transports that answer a targeted flush.
    pub(crate) flush_hooks: parking_lot::RwLock<Vec<tokio::sync::mpsc::Sender<FlushTarget>>>,
    /// Ready members: member -> the connection that made it ready.
    pub(crate) ready: parking_lot::RwLock<HashMap<String, String>>,
    /// Speech waiting for `media_ready`, in order.
    pub(crate) pending_speech: parking_lot::Mutex<VecDeque<SpeechRequest>>,
    pub(crate) speech_seq: std::sync::atomic::AtomicU64,
    /// Serializes draining so queued speech keeps its order.
    pub(crate) drain: tokio::sync::Mutex<()>,
}

/// How much speech may wait for `media_ready` in one session.
pub(crate) const MAX_PENDING_SPEECH: usize = 64;

impl MediaState {
    /// Whether a request's listeners can hear it now.
    pub(crate) fn can_hear(&self, request: &SpeechRequest) -> bool {
        let ready = self.ready.read();
        if request.to.is_empty() {
            !ready.is_empty()
        } else {
            request.to.iter().all(|member| ready.contains_key(member))
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRAME: Duration = Duration::from_millis(20);

    fn written(tracker: &mut ReleaseTracker, turn: &str, chunk: u64, frames: u32, at: &mut Instant) -> Vec<ReleaseReport> {
        let mut out = Vec::new();
        for _ in 0..frames {
            *at += FRAME;
            tracker.frame_written(Some(&TurnTag::new(turn, chunk)), FRAME, *at, &mut out);
        }
        out
    }

    #[test]
    fn tag_round_trips_through_metadata_and_accepts_the_alias() {
        let mut metadata = HashMap::new();
        TurnTag::new("t1", 3).write_to(&mut metadata);
        assert_eq!(metadata[keys::TURN_ID], "t1");
        assert_eq!(metadata[keys::CHUNK], "3");
        assert_eq!(TurnTag::from_metadata(&metadata), Some(TurnTag::new("t1", 3)));
        let alias = HashMap::from([(keys::TURN_ID_ALIAS.to_string(), "t2".to_string())]);
        assert_eq!(TurnTag::from_metadata(&alias), Some(TurnTag::new("t2", 0)));
        assert_eq!(TurnTag::from_metadata(&HashMap::new()), None);
    }

    #[test]
    fn the_tag_travels_with_the_origin() {
        // The router keeps `participant.*` next to every payload, so the tag
        // on a text chunk reaches the audio produced from it.
        let mut metadata = HashMap::from([("participant.id".to_string(), "alice".to_string())]);
        TurnTag::new("t1", 2).write_to(&mut metadata);
        let origin = Arc::new(crate::transport::Origin::from_metadata(&metadata).unwrap());
        let audio = RuntimeData::Audio {
            samples: vec![0.0f32; 4].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let out = crate::transport::Routed::new(audio, Some(origin)).into_transport();
        assert_eq!(TurnTag::from_metadata(&out.metadata), Some(TurnTag::new("t1", 2)));
    }

    #[test]
    fn reports_at_most_every_interval_and_counts_chunks_behind_the_one_leaving() {
        let mut tracker = ReleaseTracker::default();
        let mut at = Instant::now();
        // Chunk 0: 400 ms. One report at the first frame, then one per 100 ms.
        let reports = written(&mut tracker, "t", 0, 20, &mut at);
        assert_eq!(reports.len(), 4);
        assert!(reports.iter().all(|r| r.chunks_released == 0 && !r.flushed));
        // The first frame of chunk 1 means chunk 0 has wholly left.
        let reports = written(&mut tracker, "t", 1, 5, &mut at);
        let last = reports.last().unwrap();
        assert_eq!((last.chunks_released, last.chunk), (1, 1));
        assert_eq!(last.released, Duration::from_millis(420));
    }

    #[test]
    fn an_interrupted_turn_reports_a_strict_prefix() {
        let mut tracker = ReleaseTracker::default();
        let mut at = Instant::now();
        written(&mut tracker, "t", 0, 20, &mut at);
        written(&mut tracker, "t", 1, 7, &mut at);
        // Chunk 1's remaining frames and all of chunk 2 are dropped unsent.
        let mut out = Vec::new();
        tracker.flushed(Some("t"), at, &mut out);
        assert_eq!(out.len(), 1);
        let report = &out[0];
        assert!(report.flushed);
        assert_eq!(report.released, Duration::from_millis(540));
        assert_eq!(report.chunks_released, 1, "chunk 1 was cut, chunk 2 never left");
        // Nothing more is reported for it.
        tracker.drained(at, &mut out);
        assert_eq!(out.len(), 1);
        // The next turn's audio does not complete the cut turn.
        let reports = written(&mut tracker, "u", 0, 3, &mut at);
        assert!(reports.iter().all(|r| &*r.turn_id == "u"), "{reports:?}");
    }

    #[test]
    fn an_end_mark_or_the_next_turn_completes_the_last_chunk() {
        let mut tracker = ReleaseTracker::default();
        let mut at = Instant::now();
        written(&mut tracker, "t", 0, 5, &mut at);
        let mut out = Vec::new();
        tracker.chunk_end(Some(&TurnTag::new("t", 0)), at, &mut out);
        assert_eq!(out.last().unwrap().chunks_released, 1);

        written(&mut tracker, "u", 0, 5, &mut at);
        written(&mut tracker, "u", 1, 5, &mut at);
        // Audio of turn v follows: u is complete, both chunks.
        let reports = written(&mut tracker, "v", 0, 1, &mut at);
        let closed = reports.iter().find(|r| &*r.turn_id == "u").unwrap();
        assert_eq!(closed.chunks_released, 2);
        assert_eq!(closed.released, Duration::from_millis(200));
    }

    #[test]
    fn a_drained_queue_is_reported_when_the_last_frame_was_already_reported() {
        let mut tracker = ReleaseTracker::default();
        let mut at = Instant::now();
        // Six frames of 20 ms: the periodic report (every 100 ms) falls on
        // the last one, so nothing is left unreported when the queue empties.
        let reports = written(&mut tracker, "t", 0, 6, &mut at);
        assert_eq!(reports.last().unwrap().released, Duration::from_millis(120));
        assert!(!reports.last().unwrap().drained);
        let mut out = Vec::new();
        tracker.drained(at, &mut out);
        assert_eq!(out.len(), 1, "the drained state must still be reported");
        assert!(out[0].drained);
        assert_eq!(out[0].released, Duration::from_millis(120));
        // Once: an idle sender polls its empty queue again and again.
        tracker.drained(at, &mut out);
        tracker.drained(at, &mut out);
        assert_eq!(out.len(), 1);
        // More audio of the turn, then empty again: reported again.
        written(&mut tracker, "t", 0, 1, &mut at);
        tracker.drained(at, &mut out);
        assert_eq!(out.len(), 2);
        assert!(out[1].drained);
    }

    #[test]
    fn a_drained_queue_and_a_flush_of_an_unsent_turn_are_reported() {
        let mut tracker = ReleaseTracker::default();
        let mut at = Instant::now();
        written(&mut tracker, "t", 0, 3, &mut at);
        let mut out = Vec::new();
        tracker.drained(at, &mut out);
        assert!(out[0].drained);
        assert_eq!(out[0].released, Duration::from_millis(60));
        assert_eq!(out[0].chunks_released, 0, "the chunk may have more audio coming");
        // A turn flushed before any of it was written reports zero, and does
        // not close the turn being sent.
        tracker.flushed(Some("later"), at, &mut out);
        assert_eq!((&*out[1].turn_id, out[1].released, out[1].flushed), ("later", Duration::ZERO, true));
        let reports = written(&mut tracker, "t", 0, 5, &mut at);
        assert!(reports.iter().all(|r| &*r.turn_id == "t" && r.chunks_released == 0));
        // A session-wide flush reports the turn being sent.
        tracker.flushed(None, at, &mut out);
        assert_eq!((&*out[2].turn_id, out[2].released), ("t", Duration::from_millis(160)));
        let reports = written(&mut tracker, "u", 0, 1, &mut at);
        assert!(reports.iter().all(|r| &*r.turn_id == "u"), "{reports:?}");
    }

    #[test]
    fn segments_keep_tags_across_reframing() {
        let mut segments = TurnSegments::default();
        let (a, b) = (TurnTag::new("t", 0), TurnTag::new("t", 1));
        segments.push(Some(&a), 1500);
        segments.push(Some(&b), 500);
        segments.push(None, 960);
        assert_eq!(segments.take(960), Some(a.clone()));
        // Straddles the chunk boundary: tagged by its first sample.
        assert_eq!(segments.take(960), Some(a));
        assert!(!segments.only_turn("t"));
        assert_eq!(segments.take(80), Some(b));
        assert_eq!(segments.take(960), None);
        assert!(segments.is_empty());
    }

    #[test]
    fn media_events_have_the_documented_json_shape() {
        let ready = MediaEvent::MediaReady {
            member: "alice".into(),
            transport: "webrtc".into(),
            ts_ms: 7,
        };
        assert_eq!(
            serde_json::to_value(&ready).unwrap(),
            serde_json::json!({"kind":"media_ready","member":"alice","transport":"webrtc","ts_ms":7})
        );
        let released = MediaEvent::AudioReleased(AudioReleased {
            member: "alice".into(),
            turn_id: "t".into(),
            released_ms: 540,
            chunks_released: 1,
            chunk: 1,
            flushed: true,
            drained: false,
            stream_id: None,
        });
        assert_eq!(
            serde_json::to_value(&released).unwrap(),
            serde_json::json!({"kind":"audio_released","member":"alice","turn_id":"t",
                "released_ms":540,"chunks_released":1,"chunk":1,"flushed":true,"drained":false})
        );
        assert_eq!(MediaEvent::from_runtime(&released.to_runtime()), Some(released));
    }
}
