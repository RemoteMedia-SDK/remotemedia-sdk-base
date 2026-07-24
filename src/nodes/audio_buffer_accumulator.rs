use crate::data::AudioBuffer as ProtoAudioBuffer;
/// Audio Buffer Accumulator Node
///
/// Accumulates audio chunks during speech and releases complete utterances
/// when VAD detects silence. Works in conjunction with SileroVADNode.
///
/// Pipeline flow:
///   Microphone → [Audio] → AudioBufferAccumulator
///   Microphone → [Audio] → SileroVAD → [JSON] → AudioBufferAccumulator
///   AudioBufferAccumulator → [Audio] → LFM2Audio → Kokoro
///
/// The accumulator receives:
/// - Audio chunks (from microphone)
/// - VAD events (from SileroVAD)
///
/// When speech ends (is_speech_end=true), it outputs the accumulated audio buffer.
use crate::data::{ControlMessageType, RuntimeData};
use crate::error::{Error, Result};
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use std::sync::Arc;

/// Buffer state for a single session
#[derive(Debug, Clone)]
struct BufferState {
    /// Accumulated audio samples (f32)
    accumulated_samples: Vec<f32>,
    /// Sample rate of accumulated audio
    sample_rate: u32,
    /// Number of channels
    channels: u32,
    /// Is speech currently active
    is_speaking: bool,
    /// Total chunks accumulated
    chunks_accumulated: usize,
    /// Copy of the last emitted utterance's samples
    last_emitted_samples: Vec<f32>,
    /// Instant when the last utterance was emitted
    last_emitted_at: Option<std::time::Instant>,
    /// Last seen metadata for this buffer
    last_metadata: Option<serde_json::Value>,
    /// Last seen stream_id for this buffer
    last_stream_id: Option<String>,
}

impl Default for BufferState {
    fn default() -> Self {
        Self {
            accumulated_samples: Vec::new(),
            sample_rate: 24000,
            channels: 1,
            is_speaking: false,
            chunks_accumulated: 0,
            last_emitted_samples: Vec::new(),
            last_emitted_at: None,
            last_metadata: None,
            last_stream_id: None,
        }
    }
}

/// Audio Buffer Accumulator Node
pub struct AudioBufferAccumulatorNode {
    /// Minimum buffer duration in ms before allowing output
    min_utterance_duration_ms: u32,

    /// Maximum buffer duration in ms (safety limit)
    max_utterance_duration_ms: u32,

    /// Emit a CancelSpeculation control message when speech starts.
    emit_cancel_on_speech_start: bool,

    /// Buffer states per session
    states: Arc<Mutex<std::collections::HashMap<String, BufferState>>>,

    /// Pending audio chunks waiting for VAD event (per session).
    /// Each entry carries an arrival-time `Instant` so old chunks can
    /// be age-evicted: without that, a partial utterance whose tail
    /// never triggered `speech_end` (the user stopped mid-sentence,
    /// or the assistant's own output drowned out their trailing
    /// words) leaves 640 ms of stale speech in pending — which later
    /// gets glued onto a brand-new utterance as a phantom prefix the
    /// STT transcribes as if it were part of the current turn.
    pending_audio: Arc<
        Mutex<std::collections::HashMap<String, Vec<(Vec<f32>, u32, u32, std::time::Instant)>>>,
    >,
}

/// Hard cap on how old a chunk sitting in `pending_audio` may be before
/// it is discarded. The prefix-padding we glue onto a new utterance at
/// `speech_start` is only useful when it actually precedes the current
/// speech — anything older than this window is guaranteed stale and
/// only serves to contaminate the next transcript. 500 ms sits just
/// beyond the 20-chunk count cap (~640 ms) so normal continuous
/// capture still gets its pre-roll.
const MAX_PENDING_AGE_MS: u128 = 500;

impl AudioBufferAccumulatorNode {
    pub fn new(
        min_utterance_duration_ms: Option<u32>,
        max_utterance_duration_ms: Option<u32>,
        emit_cancel_on_speech_start: Option<bool>,
    ) -> Self {
        Self {
            min_utterance_duration_ms: min_utterance_duration_ms.unwrap_or(250),
            max_utterance_duration_ms: max_utterance_duration_ms.unwrap_or(30000),
            emit_cancel_on_speech_start: emit_cancel_on_speech_start.unwrap_or(true),
            states: Arc::new(Mutex::new(std::collections::HashMap::new())),
            pending_audio: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn convert_audio_to_f32(&self, audio_buf: &ProtoAudioBuffer) -> Result<Vec<f32>> {
        match audio_buf.format {
            1 => {
                // AUDIO_FORMAT_F32
                let sample_count = audio_buf.samples.len() / 4;
                Ok((0..sample_count)
                    .map(|i| {
                        let offset = i * 4;
                        f32::from_le_bytes([
                            audio_buf.samples[offset],
                            audio_buf.samples[offset + 1],
                            audio_buf.samples[offset + 2],
                            audio_buf.samples[offset + 3],
                        ])
                    })
                    .collect())
            }
            2 => {
                // AUDIO_FORMAT_I16
                let sample_count = audio_buf.samples.len() / 2;
                Ok((0..sample_count)
                    .map(|i| {
                        let offset = i * 2;
                        let i16_val = i16::from_le_bytes([
                            audio_buf.samples[offset],
                            audio_buf.samples[offset + 1],
                        ]);
                        i16_val as f32 / 32768.0
                    })
                    .collect())
            }
            _ => Err(Error::Execution(format!(
                "Unsupported audio format: {} (expected 1=F32 or 2=I16)",
                audio_buf.format
            ))),
        }
    }

    #[allow(dead_code)] // Reserved for byte-format audio output support
    fn convert_f32_to_bytes(&self, samples: &[f32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(samples.len() * 4);
        for &sample in samples {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        bytes
    }

    fn handle_audio_chunk(
        &self,
        audio_buf: &ProtoAudioBuffer,
        session_id: &str,
        states: &mut std::collections::HashMap<String, BufferState>,
        pending: &mut std::collections::HashMap<
            String,
            Vec<(Vec<f32>, u32, u32, std::time::Instant)>,
        >,
        metadata: Option<&serde_json::Value>,
        stream_id: Option<&str>,
    ) -> Result<Vec<RuntimeData>> {
        let samples = self.convert_audio_to_f32(audio_buf)?;

        // Get or create state for this session
        let state = states
            .entry(session_id.to_string())
            .or_insert_with(BufferState::default);

        if let Some(m) = metadata {
            state.last_metadata = Some(m.clone());
        }
        if let Some(s) = stream_id {
            state.last_stream_id = Some(s.to_string());
        }

        // Check if we're currently speaking (have received speech_start)
        if state.is_speaking {
            // Accumulate this chunk
            state.accumulated_samples.extend_from_slice(&samples);
            state.sample_rate = audio_buf.sample_rate;
            state.channels = audio_buf.channels;
            state.chunks_accumulated += 1;

            tracing::debug!(
                "[AudioBuffer] Session {}: Accumulated chunk {} ({} total samples)",
                session_id,
                state.chunks_accumulated,
                state.accumulated_samples.len()
            );

            // Check if we've hit the maximum duration (safety limit)
            let duration_ms =
                (state.accumulated_samples.len() as f32 / state.sample_rate as f32 * 1000.0) as u32;
            if duration_ms >= self.max_utterance_duration_ms {
                tracing::warn!(
                    "[AudioBuffer] Session {}: Hit max duration {}ms, forcing output \
                     (the user likely spoke past the cap, or VAD never reported \
                     speech_end — feedback / continuous noise are common causes)",
                    session_id,
                    duration_ms
                );

                // Force output. `flush_buffer` resets `is_speaking` to
                // false, so any audio that arrives between this flush
                // and the next VAD `speech_start` would land in the
                // `pending` ring and age out before being heard. Keep
                // `is_speaking = true` after the flush so the next
                // chunk re-opens accumulation immediately and the
                // user's continuous utterance survives the cap.
                let outs = self.flush_buffer(state, session_id)?;
                state.is_speaking = true;
                state.accumulated_samples.clear();
                state.chunks_accumulated = 0;
                return Ok(outs);
            }

            Ok(vec![])
        } else {
            // Not speaking yet, buffer this audio in pending for speech padding
            tracing::trace!(
                "[AudioBuffer] Session {}: Buffering audio chunk in pending (not speaking yet)",
                session_id
            );

            let pending_vec = pending
                .entry(session_id.to_string())
                .or_insert_with(Vec::new);

            let now = std::time::Instant::now();
            pending_vec.push((samples, audio_buf.sample_rate, audio_buf.channels, now));

            // Age-based eviction: drop anything older than the
            // pre-roll window. This is the primary defence against
            // stale audio leaking into a later utterance when the
            // chunk arrival rate drops (mic muted, network hiccup,
            // client paused). Count-based cap below still runs as a
            // belt-and-braces size limit.
            pending_vec
                .retain(|(_, _, _, t)| now.duration_since(*t).as_millis() <= MAX_PENDING_AGE_MS);

            // Keep only last 20 chunks for speech padding (~640ms at 16kHz/512 samples)
            // This prevents accumulating minutes of silence between utterances
            let max_padding_chunks = 20;
            if pending_vec.len() > max_padding_chunks {
                pending_vec.drain(0..(pending_vec.len() - max_padding_chunks));
            }

            Ok(vec![])
        }
    }

    fn handle_vad_event(
        &self,
        vad_json: &serde_json::Value,
        session_id: &str,
        states: &mut std::collections::HashMap<String, BufferState>,
        pending: &mut std::collections::HashMap<
            String,
            Vec<(Vec<f32>, u32, u32, std::time::Instant)>,
        >,
    ) -> Result<Vec<RuntimeData>> {
        fn nested_bool(value: &serde_json::Value, key: &str) -> bool {
            value.get(key).and_then(|v| v.as_bool()).unwrap_or_else(|| {
                ["payload", "data", "event"].iter().any(|nested_key| {
                    value
                        .get(nested_key)
                        .and_then(|nested| nested.get(key))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
            })
        }

        let is_speech_start = nested_bool(vad_json, "is_speech_start");
        let is_speech_end = nested_bool(vad_json, "is_speech_end");

        tracing::debug!(
            "[AudioBuffer] Session {}: VAD event - start={}, end={}",
            session_id,
            is_speech_start,
            is_speech_end
        );

        if is_speech_start {
            // Start accumulating audio
            let state = states
                .entry(session_id.to_string())
                .or_insert_with(BufferState::default);
            state.is_speaking = true;

            let mut restored = false;
            if let Some(emitted_at) = state.last_emitted_at {
                let elapsed_ms = emitted_at.elapsed().as_millis();
                if elapsed_ms < 3000 {
                    // 3.0 seconds continuation threshold
                    tracing::info!(
                        "[AudioBuffer] Session {}: Early user barge-in/continuation detected ({}ms since last utterance) — restoring previous audio buffer ({} samples) for combined turn",
                        session_id,
                        elapsed_ms,
                        state.last_emitted_samples.len()
                    );
                    state.accumulated_samples = state.last_emitted_samples.clone();
                    restored = true;
                }
            }

            if !restored {
                state.accumulated_samples.clear();
                state.chunks_accumulated = 0;
            }

            // Reset last emitted state to prevent double-restoring or compounding
            state.last_emitted_samples.clear();
            state.last_emitted_at = None;

            // Add pending chunks that contain the beginning of
            // speech, but FIRST drop any chunk older than the pre-roll
            // window — the count-based cap alone lets a partial prior
            // utterance linger here when chunk arrival stalls, and
            // that stale audio would otherwise be glued onto this
            // brand-new utterance as a phantom prefix that Whisper
            // transcribes as if it were part of the current turn.
            if let Some(mut pending_chunks) = pending.remove(session_id) {
                let now = std::time::Instant::now();
                let before = pending_chunks.len();
                pending_chunks.retain(|(_, _, _, t)| {
                    now.duration_since(*t).as_millis() <= MAX_PENDING_AGE_MS
                });
                let evicted = before - pending_chunks.len();
                if evicted > 0 {
                    tracing::info!(
                        "[AudioBuffer] Session {}: dropped {} stale pending chunk(s) (> {} ms old)",
                        session_id,
                        evicted,
                        MAX_PENDING_AGE_MS,
                    );
                }

                tracing::info!(
                    "[AudioBuffer] Session {}: Speech started, adding {} pending chunks to buffer",
                    session_id,
                    pending_chunks.len()
                );

                for (samples, sr, ch, _t) in pending_chunks {
                    state.accumulated_samples.extend_from_slice(&samples);
                    state.sample_rate = sr;
                    state.channels = ch;
                    state.chunks_accumulated += 1;
                }
            } else {
                tracing::info!(
                    "[AudioBuffer] Session {}: Speech started, no pending chunks",
                    session_id
                );
            }

            if self.emit_cancel_on_speech_start {
                Ok(vec![RuntimeData::ControlMessage {
                    message_type: ControlMessageType::CancelSpeculation {
                        from_timestamp: 0,
                        to_timestamp: 0,
                    },
                    segment_id: None,
                    timestamp_ms: 0,
                    metadata: serde_json::Value::Null,
                }])
            } else {
                Ok(vec![])
            }
        } else if is_speech_end {
            // Always clear pending on speech_end, whether or not
            // `state.is_speaking` happens to be true. If the two go
            // out of sync (VAD re-emits end without a preceding
            // start, state was reset by barge-in, etc.), leaving
            // pending populated means stale audio gets glued onto
            // the NEXT utterance as a phantom prefix. This is the
            // failure mode behind "I said that a minute ago" —
            // the user's untriggered tail was parked in pending and
            // re-emerged when the next speech_start fired.
            if let Some(old_pending) = pending.remove(session_id) {
                if !old_pending.is_empty() {
                    tracing::info!(
                        "[AudioBuffer] Session {}: Cleared {} pending chunks on speech_end",
                        session_id,
                        old_pending.len()
                    );
                }
            }

            if let Some(state) = states.get_mut(session_id) {
                if state.is_speaking {
                    tracing::info!(
                        "[AudioBuffer] Session {}: Speech ended, outputting {} samples ({} chunks)",
                        session_id,
                        state.accumulated_samples.len(),
                        state.chunks_accumulated
                    );
                    return self.flush_buffer(state, session_id);
                }
            }
            Ok(vec![])
        } else {
            // No action needed for intermediate VAD frames
            Ok(vec![])
        }
    }

    fn flush_buffer(
        &self,
        state: &mut BufferState,
        session_id: &str,
    ) -> Result<Vec<RuntimeData>> {
        if state.accumulated_samples.is_empty() {
            // Promoted to INFO: flushing with no samples means VAD
            // emitted speech_end before any audio chunks arrived in
            // the speaking window — typically a VAD/audio race or a
            // dropped chunk. Worth seeing in the logs when the user
            // reports "I spoke but it didn't transcribe".
            tracing::info!(
                "[AudioBuffer] Session {}: Flush requested but accumulator is empty — \
                 audio path may have raced VAD events or dropped chunks",
                session_id,
            );
            state.is_speaking = false;
            return Ok(vec![]);
        }

        // Check minimum duration
        let duration_ms =
            (state.accumulated_samples.len() as f32 / state.sample_rate as f32 * 1000.0) as u32;
        if duration_ms < self.min_utterance_duration_ms {
            // Promoted to INFO: this is the most common silent-drop
            // path. With it at debug! a "my mic isn't transcribing"
            // report is impossible to triage from the logs.
            tracing::info!(
                "[AudioBuffer] Session {}: Utterance too short ({}ms < {}ms), discarding",
                session_id,
                duration_ms,
                self.min_utterance_duration_ms
            );
            state.accumulated_samples.clear();
            state.is_speaking = false;
            state.chunks_accumulated = 0;
            return Ok(vec![]);
        }

        // Energy gate — reject silent / near-silent utterances. Silero
        // occasionally reports high speech-probability on ambient
        // self-noise from the mic capture layer (condenser hiss,
        // codec residue, etc.); the resulting near-silent flush
        // then feeds Whisper, which is famous for hallucinating
        // "Thank you." / "Bye." / "you." on silence. Kill both
        // failure modes at the source by dropping the flush if peak
        // amplitude is below a threshold. 0.02 ≈ -34 dBFS — well
        // below any real speech level, but above codec dither.
        const MIN_PEAK_AMPLITUDE: f32 = 0.02;
        let peak = state
            .accumulated_samples
            .iter()
            .fold(0.0_f32, |acc, &s| acc.max(s.abs()));
        if peak < MIN_PEAK_AMPLITUDE {
            tracing::info!(
                "[AudioBuffer] Session {}: Dropping {}ms / {} samples — peak {:.4} < {:.4} (silence)",
                session_id,
                duration_ms,
                state.accumulated_samples.len(),
                peak,
                MIN_PEAK_AMPLITUDE
            );
            state.accumulated_samples.clear();
            state.is_speaking = false;
            state.chunks_accumulated = 0;
            return Ok(vec![]);
        }

        // Get accumulated samples
        let samples = state.accumulated_samples.clone();
        let num_samples = samples.len();

        // Save copy for potential smart merge-back on early user barge-in/continuation
        state.last_emitted_samples = samples.clone();
        state.last_emitted_at = Some(std::time::Instant::now());

        tracing::info!(
            "[AudioBuffer] Session {}: Flushing buffer - {}ms, {} samples (peak {:.4})",
            session_id,
            duration_ms,
            num_samples,
            peak
        );

        // Save format info before clearing
        let sample_rate = state.sample_rate;
        let channels = state.channels;

        // Clear buffer
        state.accumulated_samples.clear();
        state.is_speaking = false;
        state.chunks_accumulated = 0;

        // Emit the accumulated audio, then a flush signal so any
        // downstream STT node (e.g. streaming Whisper) finalizes
        // the utterance. Without the flush, a streaming STT node
        // buffers the audio forever and never emits text.
        let audio = RuntimeData::Audio {
            samples: samples.into(),
            sample_rate,
            channels,
            stream_id: state.last_stream_id.clone(),
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: state.last_metadata.clone(),
        };
        let flush = RuntimeData::Json(serde_json::json!({ "type": "flush" }));
        Ok(vec![audio, flush])
    }

    fn handle_control_message(
        &self,
        message_type: &ControlMessageType,
        session_id: &str,
        states: &mut std::collections::HashMap<String, BufferState>,
        pending: &mut std::collections::HashMap<
            String,
            Vec<(Vec<f32>, u32, u32, std::time::Instant)>,
        >,
    ) {
        match message_type {
            ControlMessageType::CancelSpeculation { .. } => {
                if let Some(state) = states.get_mut(session_id) {
                    tracing::info!(
                        "[AudioBuffer] Session {}: CancelSpeculation received — discarding \
                         accumulated buffer ({} samples)",
                        session_id,
                        state.accumulated_samples.len()
                    );
                    state.accumulated_samples.clear();
                    state.is_speaking = false;
                    state.chunks_accumulated = 0;
                }
                if let Some(pending_vec) = pending.get_mut(session_id) {
                    if !pending_vec.is_empty() {
                        tracing::info!(
                            "[AudioBuffer] Session {}: Cleared {} pending chunks on CancelSpeculation",
                            session_id,
                            pending_vec.len()
                        );
                        pending_vec.clear();
                    }
                }
            }
            _ => {}
        }
    }
}

// Phase A-Wave 3: migrated to `SyncStreamingNode`. Previously held two
// `tokio::sync::Mutex` locks across `handle_audio_chunk(...).await` /
// `handle_vad_event(...).await` — but those helpers never actually
// awaited anything internally, so the async annotations and `.await`s
// were vestigial. The lock-across-await hazard is gone with
// `parking_lot::Mutex` + sync helpers. The collect-then-fire
// discipline is preserved: all state mutation happens under the two
// locks, which are dropped before the callback fires.
impl SyncStreamingNode for AudioBufferAccumulatorNode {
    fn node_type(&self) -> &str {
        "AudioBufferAccumulatorNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "AudioBufferAccumulatorNode requires streaming mode - \
             callers must use process_streaming() (the router does this \
             automatically when the factory declares is_multi_output_streaming=true)"
                .into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<()>,
    ) -> Result<usize> {
        let session_key = session_id.unwrap_or("default").to_string();

        // Collect output under the lock, release, then fire callback.
        // Both locks are `parking_lot::Mutex`; uncontended fast path
        // is a single CAS. No `.await` anywhere in this method; the
        // old async annotations were pure overhead.
        let output: Vec<RuntimeData> = {
            let mut states = self.states.lock();
            let mut pending = self.pending_audio.lock();

            match &data {
                RuntimeData::Audio {
                    samples,
                    sample_rate,
                    channels,
                    stream_id,
                    metadata,
                    ..
                } => {
                    // Convert to the legacy ProtoAudioBuffer shape the
                    // helpers expect. One heap op per call — unchanged
                    // from the previous implementation.
                    let audio_buf = crate::data::AudioBuffer {
                        samples: samples.iter().flat_map(|f| f.to_le_bytes()).collect(),
                        sample_rate: *sample_rate,
                        channels: *channels,
                        format: 1, // F32
                        num_samples: samples.len() as u64,
                    };
                    self.handle_audio_chunk(
                        &audio_buf,
                        &session_key,
                        &mut states,
                        &mut pending,
                        metadata.as_ref(),
                        stream_id.as_deref(),
                    )?
                }
                RuntimeData::Json(json_value) => {
                    self.handle_vad_event(json_value, &session_key, &mut states, &mut pending)?
                }
                RuntimeData::ControlMessage { message_type, .. } => {
                    self.handle_control_message(
                        message_type,
                        &session_key,
                        &mut states,
                        &mut pending,
                    );
                    vec![data.clone()]
                }
                _ => {
                    tracing::warn!("[AudioBuffer] Received unexpected data type");
                    vec![]
                }
            }
        };

        let count = output.len();
        for out in output {
            callback(out)?;
        }
        Ok(count)
    }
}

impl AudioBufferAccumulatorNode {
    /// Initialize the audio buffer accumulator
    pub async fn initialize(&mut self) -> Result<()> {
        tracing::info!(
            "[AudioBufferAccumulator] Initialized (min={}ms, max={}ms)",
            self.min_utterance_duration_ms,
            self.max_utterance_duration_ms
        );
        Ok(())
    }

    /// Check if this node is stateful
    pub fn is_stateful(&self) -> bool {
        true
    }

    /// Reset the buffer state.
    ///
    /// Previously required `tokio::task::block_in_place` +
    /// `blocking_lock` because the state was behind a
    /// `tokio::sync::Mutex`. After A-Wave 3 those are
    /// `parking_lot::Mutex` — plain sync `.lock()` works from any
    /// thread.
    pub fn reset_state(&mut self) {
        self.states.lock().clear();
        self.pending_audio.lock().clear();
        tracing::info!("[AudioBufferAccumulator] States reset");
    }
}
