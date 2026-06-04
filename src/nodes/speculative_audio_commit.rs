//! Speculative Audio Commit Node
//!
//! Drop-in replacement for `AudioBufferAccumulatorNode` designed to
//! work downstream of `SpeculativeVADCoordinator`. It is responsible
//! for deciding *when* to commit a speech utterance to STT — which is
//! distinct from VAD's per-frame "is there speech" decision.
//!
//! ## Why this exists
//!
//! The legacy accumulator flushes to STT on the FIRST `is_speech_end`
//! VAD event. That works for clean speech but fragments natural
//! mid-sentence pauses: a user pausing 1.0–2.0 s between clauses
//! (very common when composing a question) trips VAD's silence
//! threshold (typically 800–1000 ms) and the utterance is split into
//! two, with each half going to STT separately. The downstream LLM
//! then sees two short user turns instead of one coherent question.
//!
//! ## Design
//!
//! Three input streams are received on the main port:
//!
//! - `RuntimeData::Audio` — chunks streamed by upstream (the
//!   coordinator's immediate-forwarding path).
//! - `RuntimeData::Json` — VAD events tagged
//!   `is_speech_start` / `is_speech_end`.
//! - `RuntimeData::ControlMessage(CancelSpeculation { from, to })` —
//!   emitted by `SpeculativeVADCoordinator` when a forwarded segment
//!   was retroactively determined to be a false positive (shorter
//!   than `min_speech_duration_ms`).
//!
//! The node maintains a per-session sample buffer plus a small state
//! machine:
//!
//! ```text
//!                       speech_start              speech_end
//!   IDLE ──────────────► SPEAKING ─────────────► PENDING_COMMIT
//!     ▲                     ▲                          │
//!     │                     │ speech_start             │ commit_delay_ms elapses
//!     │                     │ (merge!)                 │
//!     │                     └──────────────────────────┘
//!     │                                                │
//!     │                                                ▼
//!     │                                            COMMIT (flush to STT)
//!     │                                                │
//!     └────────────── ControlMessage(Cancel) ──────────┘
//! ```
//!
//! `PENDING_COMMIT → SPEAKING` on a new `speech_start` is the
//! mid-sentence-pause merge: the buffer is preserved so the two
//! halves arrive at STT as a single utterance.
//!
//! `IDLE` and `PENDING_COMMIT` both maintain a sliding pre-roll
//! window (last `pre_roll_ms` of audio) so that when speech does
//! start, the first 100–250 ms of phonemes — which Silero VAD
//! typically classifies as "uncertain" before crossing the threshold —
//! are still captured.
//!
//! Commit is timer-driven by *audio-time* (cumulative sample count)
//! rather than wall-clock — so a stalled chunk feed doesn't fire
//! commit prematurely, and a burst of chunks can't overshoot it. The
//! node stays sync and lock-narrow (matches the AudioBufferAccumulator
//! pattern). Audio chunks arrive at ~50 fps (20 ms each), so the
//! worst-case commit-fire jitter is one chunk duration.

use crate::data::{ControlMessageType, RuntimeData};
use crate::error::{Error, Result};
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Per-session state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CommitState {
    /// All currently held audio. While IDLE this is a sliding
    /// pre-roll; while SPEAKING this is the utterance being captured;
    /// while PENDING_COMMIT this is the captured utterance + post-end
    /// silence + (potentially) merged continuation.
    samples: Vec<f32>,
    sample_rate: u32,
    channels: u32,

    /// Position in `samples` where the current (or most recent)
    /// utterance started. Used so the pre-roll trim never eats into
    /// the captured utterance.
    utterance_start_idx: usize,

    /// True between a `speech_start` and a `speech_end`. PENDING_COMMIT
    /// is encoded as `speech_active == false && commit_at_samples.is_some()`.
    speech_active: bool,

    /// When set, we're in PENDING_COMMIT: hold the buffer until
    /// `cumulative_samples` reaches this many samples. Counted in
    /// audio-time rather than wall-clock so chunk-arrival pauses
    /// (network hiccup, mic stalls) don't fire commit prematurely.
    /// A `speech_start` before this is reached cancels it
    /// (mid-sentence pause merge).
    commit_at_samples: Option<u64>,

    /// Cumulative sample count since session start. Used both as
    /// the audio-time clock for `commit_at_samples` and to match
    /// `CancelSpeculation { from_timestamp, to_timestamp }` ranges
    /// against currently buffered audio.
    cumulative_samples: u64,

    /// Cumulative sample index of `samples[0]`. `cumulative_samples
    /// - samples.len() == buffer_first_sample` (modulo concurrent
    /// modification, which doesn't happen here — single-task
    /// serialised access).
    buffer_first_sample: u64,
}

impl Default for CommitState {
    fn default() -> Self {
        Self {
            samples: Vec::new(),
            sample_rate: 16_000,
            channels: 1,
            utterance_start_idx: 0,
            speech_active: false,
            commit_at_samples: None,
            cumulative_samples: 0,
            buffer_first_sample: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

pub struct SpeculativeAudioCommitNode {
    /// How long to wait after `speech_end` before flushing to STT.
    /// A new `speech_start` inside this window cancels the timer and
    /// merges the resumed speech into the same utterance. Must be
    /// long enough to cover a natural mid-sentence think-pause
    /// (typically 1.0–2.0 s) without being so long that
    /// genuine end-of-turn feels laggy.
    commit_delay_ms: u32,

    /// Sliding pre-roll window kept while IDLE. The first phonemes
    /// of an utterance often score below VAD's speech threshold
    /// before crossing it; without pre-roll the captured audio
    /// starts mid-word. 200 ms is a common default and matches
    /// Silero's typical lookback.
    pre_roll_ms: u32,

    /// Reject utterances shorter than this on commit. Stops VAD
    /// glitches from generating empty/near-empty STT calls.
    min_utterance_duration_ms: u32,

    /// Force-flush utterances that exceed this duration (safety
    /// limit against runaway capture).
    max_utterance_duration_ms: u32,

    states: Arc<Mutex<HashMap<String, CommitState>>>,
}

impl SpeculativeAudioCommitNode {
    pub fn new(
        commit_delay_ms: Option<u32>,
        pre_roll_ms: Option<u32>,
        min_utterance_duration_ms: Option<u32>,
        max_utterance_duration_ms: Option<u32>,
    ) -> Self {
        Self {
            commit_delay_ms: commit_delay_ms.unwrap_or(1500),
            pre_roll_ms: pre_roll_ms.unwrap_or(200),
            min_utterance_duration_ms: min_utterance_duration_ms.unwrap_or(250),
            max_utterance_duration_ms: max_utterance_duration_ms.unwrap_or(30_000),
            states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn pre_roll_samples(&self, sample_rate: u32) -> usize {
        ((sample_rate as u64 * self.pre_roll_ms as u64) / 1000) as usize
    }

    fn duration_ms(samples: usize, sample_rate: u32) -> u32 {
        if sample_rate == 0 {
            0
        } else {
            ((samples as u64 * 1000) / sample_rate as u64) as u32
        }
    }

    /// Build the output RuntimeData for the committed utterance. The
    /// flush emits everything from `utterance_start_idx` onward —
    /// pre-roll trimming has already kept the head tight.
    fn build_committed_audio(state: &CommitState) -> RuntimeData {
        let body = state.samples[state.utterance_start_idx..].to_vec();
        RuntimeData::Audio {
            samples: body.into(),
            sample_rate: state.sample_rate,
            channels: state.channels,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    /// Reset to IDLE and trim buffer to a fresh pre-roll window. Used
    /// after a successful commit AND on `CancelSpeculation`.
    fn reset_to_idle(state: &mut CommitState, pre_roll_samples: usize) {
        state.speech_active = false;
        state.commit_at_samples = None;
        state.utterance_start_idx = 0;
        // Trim the buffer to the trailing pre-roll. The just-flushed
        // utterance is no longer needed; whatever silence/audio came
        // after it stays as the next utterance's pre-roll.
        let buf_len = state.samples.len();
        if buf_len > pre_roll_samples {
            let drop_count = buf_len - pre_roll_samples;
            state.samples.drain(..drop_count);
            state.buffer_first_sample = state.buffer_first_sample.saturating_add(drop_count as u64);
        }
        state.utterance_start_idx = state.samples.len();
        // ^ Default to "no utterance currently captured"; the next
        // `speech_start` will reset utterance_start_idx to 0 (capturing
        // the pre-roll) before any further trimming occurs.
    }

    fn handle_audio_chunk(
        &self,
        samples_in: &[f32],
        sample_rate: u32,
        channels: u32,
        session_id: &str,
        states: &mut HashMap<String, CommitState>,
    ) -> Option<RuntimeData> {
        let pre_roll_samples = self.pre_roll_samples(sample_rate);
        let state = states
            .entry(session_id.to_string())
            .or_insert_with(CommitState::default);

        // Capture stream metadata on first chunk.
        state.sample_rate = sample_rate;
        state.channels = channels;

        // 1. Advance the audio clock with the incoming chunk's samples.
        //    Commit-deadline check happens AFTER this — chunks count as
        //    silence accruing toward the commit threshold.
        state.cumulative_samples = state
            .cumulative_samples
            .saturating_add(samples_in.len() as u64);

        // 2. Commit-deadline check. Done BEFORE appending the chunk to
        //    the buffer so the chunk that triggers the flush isn't
        //    itself part of the just-committed utterance — it becomes
        //    pre-roll for the next.
        let mut emitted: Option<RuntimeData> = None;
        if let Some(commit_at) = state.commit_at_samples {
            if state.cumulative_samples >= commit_at {
                let utterance_len = state.samples.len() - state.utterance_start_idx;
                let dur_ms = Self::duration_ms(utterance_len, sample_rate);
                if dur_ms >= self.min_utterance_duration_ms {
                    tracing::info!(
                        "[SpecCommit] Session {}: commit fired (audio-time delay \
                         elapsed) — flushing {} samples ({} ms)",
                        session_id,
                        utterance_len,
                        dur_ms
                    );
                    emitted = Some(Self::build_committed_audio(state));
                } else {
                    tracing::debug!(
                        "[SpecCommit] Session {}: commit fired but utterance only \
                         {} ms (< {} ms min); dropping",
                        session_id,
                        dur_ms,
                        self.min_utterance_duration_ms
                    );
                }
                Self::reset_to_idle(state, pre_roll_samples);
            }
        }

        // 3. Append the incoming chunk to the buffer.
        state.samples.extend_from_slice(samples_in);

        // 4. While IDLE (not speaking, no pending commit), keep only
        //    a sliding pre-roll window. Don't touch the buffer in
        //    SPEAKING or PENDING_COMMIT — both are actively building
        //    the utterance.
        if !state.speech_active && state.commit_at_samples.is_none() {
            let buf_len = state.samples.len();
            if buf_len > pre_roll_samples {
                let drop_count = buf_len - pre_roll_samples;
                state.samples.drain(..drop_count);
                state.buffer_first_sample =
                    state.buffer_first_sample.saturating_add(drop_count as u64);
            }
            state.utterance_start_idx = state.samples.len();
        }

        // 5. Max-duration safety. If we're inside an utterance and
        //    the user has been talking past the safety limit, force
        //    a flush even without a speech_end.
        if state.speech_active || state.commit_at_samples.is_some() {
            let body_len = state.samples.len() - state.utterance_start_idx;
            let body_ms = Self::duration_ms(body_len, sample_rate);
            if body_ms >= self.max_utterance_duration_ms {
                tracing::warn!(
                    "[SpecCommit] Session {}: hit max utterance duration {} ms — force-flush",
                    session_id,
                    body_ms
                );
                if emitted.is_none() {
                    emitted = Some(Self::build_committed_audio(state));
                } else {
                    // Already emitted this call; the next process
                    // cycle will catch this. Drop the buffer to
                    // prevent runaway growth.
                }
                Self::reset_to_idle(state, pre_roll_samples);
            }
        }

        emitted
    }

    fn handle_vad_event(
        &self,
        vad_json: &Value,
        session_id: &str,
        states: &mut HashMap<String, CommitState>,
    ) -> Option<RuntimeData> {
        let is_speech_start = vad_json
            .get("is_speech_start")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let is_speech_end = vad_json
            .get("is_speech_end")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        // No state change → nothing to do.
        if !is_speech_start && !is_speech_end {
            return None;
        }

        let state = states
            .entry(session_id.to_string())
            .or_insert_with(CommitState::default);
        let pre_roll_samples = self.pre_roll_samples(state.sample_rate);
        let _ = pre_roll_samples; // not used here, kept for parity with chunk path

        if is_speech_start {
            // Two cases:
            //  - PENDING_COMMIT (commit_at_samples.is_some): user paused
            //    and resumed inside the window. CANCEL the deadline,
            //    keep the buffer + utterance_start_idx untouched. The
            //    two halves merge.
            //  - IDLE: brand-new utterance. utterance_start_idx jumps
            //    to 0 so the sliding pre-roll becomes the head of
            //    this utterance.
            if state.commit_at_samples.take().is_some() {
                tracing::info!(
                    "[SpecCommit] Session {}: speech resumed during commit window — \
                     merging into current utterance ({} samples already buffered)",
                    session_id,
                    state.samples.len() - state.utterance_start_idx
                );
            } else {
                state.utterance_start_idx = 0;
                tracing::info!(
                    "[SpecCommit] Session {}: speech_start — utterance begins with \
                     {} samples of pre-roll",
                    session_id,
                    state.samples.len()
                );
            }
            state.speech_active = true;
            return None;
        }

        if is_speech_end {
            if state.speech_active {
                let delay_samples = (state.sample_rate as u64 * self.commit_delay_ms as u64) / 1000;
                state.commit_at_samples =
                    Some(state.cumulative_samples.saturating_add(delay_samples));
                state.speech_active = false;
                tracing::info!(
                    "[SpecCommit] Session {}: speech_end — pending commit in {} ms \
                     ({} samples held, threshold {} samples of audio away)",
                    session_id,
                    self.commit_delay_ms,
                    state.samples.len() - state.utterance_start_idx,
                    delay_samples
                );
            } else {
                tracing::debug!(
                    "[SpecCommit] Session {}: speech_end while not speaking — ignoring",
                    session_id
                );
            }
        }

        None
    }

    fn handle_control_message(
        &self,
        message_type: &ControlMessageType,
        session_id: &str,
        states: &mut HashMap<String, CommitState>,
    ) {
        match message_type {
            ControlMessageType::CancelSpeculation { .. } => {
                let state = states
                    .entry(session_id.to_string())
                    .or_insert_with(CommitState::default);
                let pre_roll_samples = self.pre_roll_samples(state.sample_rate);
                tracing::info!(
                    "[SpecCommit] Session {}: CancelSpeculation received — discarding \
                     pending utterance ({} samples)",
                    session_id,
                    state.samples.len() - state.utterance_start_idx
                );
                // Drop the in-flight utterance entirely. Keep the
                // recent samples as pre-roll for the next utterance.
                Self::reset_to_idle(state, pre_roll_samples);
            }
            // Other control types are not for us; ignore silently.
            ControlMessageType::BatchHint { .. } | ControlMessageType::DeadlineWarning { .. } => {}
        }
    }
}

// ---------------------------------------------------------------------------
// StreamingNode wiring
// ---------------------------------------------------------------------------

impl SyncStreamingNode for SpeculativeAudioCommitNode {
    fn node_type(&self) -> &str {
        "SpeculativeAudioCommitNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "SpeculativeAudioCommitNode requires streaming mode".into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<()>,
    ) -> Result<usize> {
        let session_key = session_id.unwrap_or("default").to_string();

        let output: Option<RuntimeData> = {
            let mut states = self.states.lock();
            match &data {
                RuntimeData::Audio {
                    samples,
                    sample_rate,
                    channels,
                    ..
                } => self.handle_audio_chunk(
                    samples,
                    *sample_rate,
                    *channels,
                    &session_key,
                    &mut states,
                ),
                RuntimeData::Json(json_value) => {
                    self.handle_vad_event(json_value, &session_key, &mut states)
                }
                RuntimeData::ControlMessage { message_type, .. } => {
                    self.handle_control_message(message_type, &session_key, &mut states);
                    None
                }
                _ => {
                    tracing::trace!(
                        "[SpecCommit] Ignoring unsupported input type {:?}",
                        data.data_type()
                    );
                    None
                }
            }
        };

        if let Some(out) = output {
            callback(out)?;
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ControlMessageType;
    use serde_json::json;

    fn audio(samples: Vec<f32>, sr: u32) -> RuntimeData {
        RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: sr,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    fn vad_event(start: bool, end: bool) -> RuntimeData {
        RuntimeData::Json(json!({
            "is_speech_start": start,
            "is_speech_end": end,
        }))
    }

    /// Drives a single input through `process_streaming` and returns
    /// any emitted outputs.
    fn step(node: &SpeculativeAudioCommitNode, input: RuntimeData) -> Vec<RuntimeData> {
        let mut out = Vec::new();
        let mut cb = |d: RuntimeData| {
            out.push(d);
            Ok(())
        };
        let _ = node.process_streaming(input, Some("s"), &mut cb);
        out
    }

    #[test]
    fn merges_speech_resumed_inside_commit_window() {
        // 16 kHz so durations are easy to reason about.
        let node = SpeculativeAudioCommitNode::new(Some(500), Some(0), Some(50), None);
        // 1 chunk = 320 samples = 20 ms
        let chunk_silent = vec![0.0_f32; 320];
        let chunk_loud = vec![0.5_f32; 320];

        // speech_start
        assert!(step(&node, vad_event(true, false)).is_empty());
        // utterance #1 — 20 chunks = 400 ms loud
        for _ in 0..20 {
            assert!(step(&node, audio(chunk_loud.clone(), 16_000)).is_empty());
        }
        // speech_end
        assert!(step(&node, vad_event(false, true)).is_empty());
        // 5 chunks of silence (100 ms) — well under the 500 ms commit_delay
        for _ in 0..5 {
            assert!(step(&node, audio(chunk_silent.clone(), 16_000)).is_empty());
        }
        // speech_start again — should merge, NOT commit
        assert!(step(&node, vad_event(true, false)).is_empty());
        // utterance #2 — 10 chunks = 200 ms loud
        for _ in 0..10 {
            assert!(step(&node, audio(chunk_loud.clone(), 16_000)).is_empty());
        }
        // speech_end
        assert!(step(&node, vad_event(false, true)).is_empty());
        // Drive enough silence chunks to exceed commit_delay
        let mut emissions: Vec<RuntimeData> = Vec::new();
        for _ in 0..40 {
            // 800 ms total
            emissions.extend(step(&node, audio(chunk_silent.clone(), 16_000)));
            if !emissions.is_empty() {
                break;
            }
        }
        assert_eq!(emissions.len(), 1, "exactly one merged commit expected");
        match &emissions[0] {
            RuntimeData::Audio { samples, .. } => {
                // 20 (utt1) + 5 (silence between) + 10 (utt2) = 35 chunks
                // = 11200 samples. Allow some slack for trailing silence
                // captured before commit fired.
                assert!(
                    samples.len() >= 11_200,
                    "merged utterance too short: {}",
                    samples.len()
                );
            }
            _ => panic!("expected committed audio"),
        }
    }

    #[test]
    fn cancel_speculation_drops_pending_utterance() {
        let node = SpeculativeAudioCommitNode::new(Some(500), Some(0), Some(50), None);
        let chunk = vec![0.5_f32; 320];

        assert!(step(&node, vad_event(true, false)).is_empty());
        for _ in 0..3 {
            // 60 ms — below typical min_speech_duration thresholds,
            // simulating the false-positive case the coordinator
            // would emit CancelSpeculation for.
            assert!(step(&node, audio(chunk.clone(), 16_000)).is_empty());
        }
        // Coordinator decides this was a false positive
        let cancel = RuntimeData::ControlMessage {
            message_type: ControlMessageType::CancelSpeculation {
                from_timestamp: 0,
                to_timestamp: 960,
            },
            segment_id: Some("s_0".into()),
            timestamp_ms: 0,
            metadata: serde_json::Value::Null,
        };
        assert!(step(&node, cancel).is_empty());

        // Drive silence past commit_delay — nothing should commit
        // because the utterance was cancelled.
        let silent = vec![0.0_f32; 320];
        let mut total = 0;
        for _ in 0..40 {
            total += step(&node, audio(silent.clone(), 16_000)).len();
        }
        assert_eq!(total, 0, "cancelled utterance must not commit");
    }

    #[test]
    fn idle_buffer_stays_bounded_by_pre_roll() {
        let node = SpeculativeAudioCommitNode::new(Some(500), Some(100), Some(50), None);
        // 16 kHz × 100 ms = 1600 samples expected pre-roll cap
        let chunk = vec![0.0_f32; 320];
        for _ in 0..200 {
            // 40 s of idle audio
            assert!(step(&node, audio(chunk.clone(), 16_000)).is_empty());
        }
        let states = node.states.lock();
        let s = states.get("s").expect("session state");
        assert!(
            s.samples.len() <= 1600 + 320,
            "idle buffer exceeded pre-roll bound: {}",
            s.samples.len()
        );
    }
}
