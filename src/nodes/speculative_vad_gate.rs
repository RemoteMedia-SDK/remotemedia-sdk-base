//! Speculative VAD Gate Node
//!
//! This node implements the core speculative forwarding strategy for low-latency voice interaction.
//! It forwards audio immediately (speculative) while buffering it in a ring buffer, then emits
//! a CancelSpeculation control message if the VAD determines the segment was a false positive.
//!
//! Key behaviors:
//! 1. **Immediate forwarding**: Audio chunks are forwarded immediately without waiting for VAD decision
//! 2. **Ring buffer storage**: Audio is stored in a lock-free ring buffer for potential cancellation
//! 3. **VAD decision handling**: After lookahead period, check VAD result and emit cancellation if needed
//! 4. **Segment tracking**: Track speculative segments with SpeculativeSegment metadata
//! 5. **Metrics tracking**: Record speculation acceptance rate for monitoring

use crate::data::{ControlMessageType, RuntimeData, SpeculativeSegment};
use crate::executor::latency_metrics::LatencyMetrics;
use crate::Error;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Per-session state for speculative VAD processing
#[derive(Debug)]
struct SessionState {
    /// Audio sample buffer for lookback (circular buffer using VecDeque)
    audio_buffer: VecDeque<f32>,

    /// Maximum buffer capacity (based on lookback window)
    buffer_capacity: usize,

    /// Current speculative segments being tracked
    #[allow(dead_code)] // Reserved for speculative segment tracking (spec 007)
    segments: Vec<SpeculativeSegment>,

    /// Total samples processed in this session
    total_samples: usize,

    /// Current segment counter
    segment_counter: u64,

    /// Is speech currently active
    #[allow(dead_code)] // Reserved for speech state tracking (spec 007)
    speech_active: bool,

    /// Samples of silence accumulated
    #[allow(dead_code)] // Reserved for silence detection (spec 007)
    silence_samples: usize,

    /// Speculation metrics
    speculations_accepted: u64,
    speculations_cancelled: u64,
}

impl SessionState {
    fn new(buffer_capacity: usize) -> Self {
        Self {
            audio_buffer: VecDeque::with_capacity(buffer_capacity),
            buffer_capacity,
            segments: Vec::new(),
            total_samples: 0,
            segment_counter: 0,
            speech_active: false,
            silence_samples: 0,
            speculations_accepted: 0,
            speculations_cancelled: 0,
        }
    }

    /// Calculate speculation acceptance rate
    fn acceptance_rate(&self) -> f64 {
        let total = self.speculations_accepted + self.speculations_cancelled;
        if total == 0 {
            return 1.0;
        }
        self.speculations_accepted as f64 / total as f64
    }
}

/// VAD decision result
#[derive(Debug, Clone)]
pub struct VADResult {
    /// Is this the end of a speech segment
    pub is_speech_end: bool,

    /// Was the speech confirmed (true) or false positive (false)
    pub is_confirmed_speech: bool,

    /// VAD confidence score (0.0-1.0)
    pub confidence: f32,

    /// Number of samples in the chunk that triggered this VAD result
    pub samples_in_chunk: usize,
}

/// Speculative VAD Gate Node
///
/// Implements speculative forwarding strategy:
/// 1. Forward audio immediately
/// 2. Store in ring buffer
/// 3. Check VAD decision after lookahead
/// 4. Emit cancellation if false positive
#[crate::node(
    node_type = "SpeculativeVADGate",
    category = "audio",
    description = "Speculative VAD gate for low-latency voice interaction",
    accepts = "audio",
    produces = "audio,control",
    multi_output
)]
pub struct SpeculativeVADGate {
    /// Lookback window in milliseconds (how much audio to keep for cancellation)
    #[config(default = 150)]
    pub lookback_ms: u32,

    /// Lookahead window in milliseconds (how long to wait before confirming speculation)
    #[config(default = 50)]
    pub lookahead_ms: u32,

    /// Sample rate of audio (needed for time calculations)
    #[config(default = 16000)]
    pub sample_rate: u32,

    /// VAD confidence threshold for speech detection (0.0-1.0)
    #[config(default = 0.5)]
    pub vad_threshold: f32,

    /// Minimum speech duration in milliseconds to trigger forwarding
    #[config(default = 250)]
    pub min_speech_ms: u32,

    /// Minimum silence duration in milliseconds to end speech segment
    #[config(default = 100)]
    pub min_silence_ms: u32,

    /// Padding before/after speech in milliseconds
    #[config(default = 30)]
    pub pad_ms: u32,

    /// Per-session state (keyed by session_id)
    #[state(default = Arc::new(Mutex::new(HashMap::new())))]
    sessions: Arc<Mutex<HashMap<String, SessionState>>>,

    /// Latency metrics for tracking speculation performance
    #[state(default = Arc::new(LatencyMetrics::new("speculative_vad_gate").unwrap()))]
    metrics: Arc<LatencyMetrics>,
}

impl SpeculativeVADGate {
    /// Process streaming implementation - required for multi_output nodes
    pub async fn process_streaming_impl<F>(
        &self,
        data: RuntimeData,
        session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let session_id = session_id.unwrap_or_else(|| "default".to_string());

        // Extract audio from RuntimeData
        let (samples, sample_rate, channels) = match &data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => (samples.clone(), *sample_rate, *channels),
            _ => {
                return Err(Error::Execution(
                    "SpeculativeVADGate requires audio input".into(),
                ))
            }
        };

        // External VAD implementations (e.g. Silero) should call process_vad_result()
        // once their asynchronous inference completes. Streaming mode forwards audio immediately.
        let vad_result = None;

        // Process the audio chunk
        let outputs = self
            .process_audio_chunk(&samples, sample_rate, channels, &session_id, vad_result)
            .await?;

        // Emit all outputs via callback
        let output_count = outputs.len();
        for output in outputs {
            callback(output)?;
        }

        Ok(output_count)
    }

    /// Ensure a session state exists, create it if missing.
    ///
    /// Returning a `MutexGuard` is deliberately avoided — the previous
    /// implementation returned one from an `async fn`, which is the
    /// classic async-lock antipattern: the caller then held it across
    /// every subsequent `.await` point. All session access is now
    /// scoped: locks are acquired inside a narrow sync block and
    /// released before any await.
    fn ensure_session_exists(&self, session_id: &str) {
        let mut sessions = self.sessions.lock();
        if !sessions.contains_key(session_id) {
            let samples_per_ms = self.config.sample_rate / 1000;
            let ring_buffer_capacity = (self.config.lookback_ms * samples_per_ms) as usize;
            sessions.insert(
                session_id.to_string(),
                SessionState::new(ring_buffer_capacity),
            );
        }
    }

    /// Process audio chunk and determine if speculation should occur
    async fn process_audio_chunk(
        &self,
        samples: &[f32],
        sample_rate: u32,
        channels: u32,
        session_id: &str,
        vad_result: Option<VADResult>,
    ) -> Result<Vec<RuntimeData>, Error> {
        let mut outputs = Vec::new();
        self.ensure_session_exists(session_id);
        // Acquire the lock for the rest of this fn. Inside this scope
        // we do only synchronous buffer ops + `handle_vad_decision`
        // (also sync) — no `.await` while the lock is held.
        let mut sessions = self.sessions.lock();
        let state = sessions.get_mut(session_id).unwrap();

        // **Step 1: Forward audio immediately (speculative)**
        let audio_output = RuntimeData::Audio {
            samples: samples.to_vec().into(),
            sample_rate,
            channels,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        outputs.push(audio_output);

        // **Step 2: Store in audio buffer for potential cancellation**
        for &sample in samples {
            if state.audio_buffer.len() >= state.buffer_capacity {
                // Buffer is full - remove oldest sample (circular buffer behavior)
                state.audio_buffer.pop_front();
            }
            state.audio_buffer.push_back(sample);
        }

        state.total_samples += samples.len();

        // **Step 3: Check VAD decision and handle false positives**
        if let Some(vad) = vad_result {
            outputs.extend(self.handle_vad_decision(state, session_id, vad));
        }

        Ok(outputs)
    }

    fn handle_vad_decision(
        &self,
        state: &mut SessionState,
        session_id: &str,
        vad: VADResult,
    ) -> Vec<RuntimeData> {
        let mut outputs = Vec::new();
        if vad.is_speech_end && !vad.is_confirmed_speech {
            // False positive detected - emit cancellation
            let segment_id = format!("{}_{}", session_id, state.segment_counter);
            state.segment_counter += 1;

            let chunk_samples = vad.samples_in_chunk.max(1);
            let from_timestamp = state.total_samples.saturating_sub(chunk_samples) as u64;
            let to_timestamp = state.total_samples as u64;

            let cancel_msg = RuntimeData::ControlMessage {
                message_type: ControlMessageType::CancelSpeculation {
                    from_timestamp,
                    to_timestamp,
                },
                segment_id: Some(segment_id),
                timestamp_ms: Self::current_timestamp_ms(),
                metadata: serde_json::json!({
                    "reason": "vad_false_positive",
                    "vad_confidence": vad.confidence,
                    "duration_samples": chunk_samples,
                }),
            };

            outputs.push(cancel_msg);

            // Update metrics
            state.speculations_cancelled += 1;
            let _ = self
                .metrics
                .set_speculation_acceptance_rate(state.acceptance_rate() * 100.0);
        } else if vad.is_speech_end && vad.is_confirmed_speech {
            // Confirmed speech - speculation accepted
            state.speculations_accepted += 1;
            let _ = self
                .metrics
                .set_speculation_acceptance_rate(state.acceptance_rate() * 100.0);

            // Clear old data from audio buffer (before this segment)
            state.audio_buffer.clear();
        }

        outputs
    }

    fn current_timestamp_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_millis(0))
            .as_millis() as u64
    }

    /// Get speculation acceptance rate for a session.
    ///
    /// Kept `async` for API stability even though the lock is now sync —
    /// callers already `.await` it. Acquires the lock briefly and
    /// releases it before returning.
    pub async fn get_acceptance_rate(&self, session_id: &str) -> f64 {
        let sessions = self.sessions.lock();
        sessions
            .get(session_id)
            .map(|s| s.acceptance_rate())
            .unwrap_or(1.0)
    }

    /// Clean up session state
    pub async fn terminate_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock();
        sessions.remove(session_id);
    }

    /// Apply a VAD result after the corresponding audio was already forwarded.
    ///
    /// The sessions lock is held only for the synchronous state update;
    /// user callbacks fire outside the lock.
    pub async fn process_vad_result<F>(
        &self,
        session_id: &str,
        vad_result: VADResult,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.ensure_session_exists(session_id);
        let outputs = {
            let mut sessions = self.sessions.lock();
            let state = sessions.get_mut(session_id).unwrap();
            self.handle_vad_decision(state, session_id, vad_result)
        };

        let count = outputs.len();
        for output in outputs {
            callback(output)?;
        }

        Ok(count)
    }
}

impl Default for SpeculativeVADGate {
    fn default() -> Self {
        Self::with_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_immediate_forwarding() {
        let gate = SpeculativeVADGate::with_default();

        let audio = RuntimeData::Audio {
            samples: vec![0.1, 0.2, 0.3].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        let mut outputs = Vec::new();
        let callback = |data: RuntimeData| {
            outputs.push(data);
            Ok(())
        };

        use crate::nodes::AsyncStreamingNode;
        let result = gate
            .process_streaming(audio, Some("test_session".to_string()), callback)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // Should emit 1 output (audio only, no cancellation)
        assert_eq!(outputs.len(), 1);

        match &outputs[0] {
            RuntimeData::Audio { samples, .. } => {
                assert_eq!(samples.len(), 3);
            }
            _ => panic!("Expected audio output"),
        }
    }

    #[tokio::test]
    async fn test_acceptance_rate_tracking() {
        let gate = SpeculativeVADGate::with_default();

        // Initially should be 1.0 (no data)
        let rate = gate.get_acceptance_rate("test").await;
        assert_eq!(rate, 1.0);

        // After processing without VAD, should still be 1.0
        let audio = RuntimeData::Audio {
            samples: vec![0.1; 100].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        use crate::nodes::AsyncStreamingNode;
        let callback = |_: RuntimeData| Ok(());
        let _ = gate
            .process_streaming(audio, Some("test".to_string()), callback)
            .await;

        let rate = gate.get_acceptance_rate("test").await;
        assert_eq!(rate, 1.0); // Still 1.0 since no VAD decisions made
    }

    #[tokio::test]
    async fn test_session_isolation() {
        let gate = Arc::new(SpeculativeVADGate::with_default());

        // Process audio from two different sessions
        for session_num in 0..2 {
            let gate_clone = gate.clone();
            let session_id = format!("session_{}", session_num);

            let audio = RuntimeData::Audio {
                samples: vec![session_num as f32; 100].into(),
                sample_rate: 16000,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            };

            use crate::nodes::AsyncStreamingNode;
            let callback = |_: RuntimeData| Ok(());
            let result = gate_clone
                .process_streaming(audio, Some(session_id.clone()), callback)
                .await;

            assert!(result.is_ok());
        }

        // Both sessions should have independent state
        let rate_0 = gate.get_acceptance_rate("session_0").await;
        let rate_1 = gate.get_acceptance_rate("session_1").await;

        assert_eq!(rate_0, 1.0);
        assert_eq!(rate_1, 1.0);
    }
}
