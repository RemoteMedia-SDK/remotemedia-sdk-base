/// Audio Channel Splitter Node
///
/// Routes audio by speaker to separate streams or channels based on
/// diarization results from SpeakerDiarizationNode.
///
/// Two output modes:
/// - Streams: Each speaker gets a separate stream_id ("speaker_0", "speaker_1", etc.)
/// - Channels: All speakers combined into multi-channel audio (speaker N = channel N)
///
/// This node expects to receive both:
/// - RuntimeData::Json with speaker segments (from SpeakerDiarizationNode)
/// - RuntimeData::Audio with the original audio
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Output mode for the audio channel splitter
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    /// Each speaker gets a separate stream_id: "speaker_0", "speaker_1", etc.
    #[default]
    Streams,
    /// All speakers combined into multi-channel audio (speaker N = channel N)
    Channels,
}

/// Audio Channel Splitter Node configuration
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct AudioChannelSplitterConfig {
    /// Output mode: "streams" (separate stream_ids) or "channels" (multi-channel)
    #[serde(alias = "outputMode")]
    pub output_mode: OutputMode,

    /// Maximum number of speakers/channels to support
    #[serde(alias = "maxSpeakers")]
    pub max_speakers: u32,

    /// Prefix for stream IDs when using streams mode
    #[serde(alias = "streamIdPrefix")]
    pub stream_id_prefix: String,

    /// Sample rate (should match input audio)
    #[serde(alias = "sampleRate")]
    pub sample_rate: u32,
}

impl Default for AudioChannelSplitterConfig {
    fn default() -> Self {
        Self {
            output_mode: OutputMode::Streams,
            max_speakers: 8,
            stream_id_prefix: "speaker".to_string(),
            sample_rate: 16000,
        }
    }
}

/// Speaker segment info from diarization
#[derive(Debug, Clone, Deserialize)]
struct DiarizationSegment {
    start: f64,
    end: f64,
    speaker: String,
}

/// Per-session state for tracking diarization results
struct SplitterState {
    /// Latest diarization segments
    segments: Vec<DiarizationSegment>,
    /// Number of speakers detected
    num_speakers: usize,
    /// Time offset (if provided by diarization)
    time_offset: f64,
}

impl Default for SplitterState {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
            num_speakers: 0,
            time_offset: 0.0,
        }
    }
}

/// Audio Channel Splitter Streaming Node
pub struct AudioChannelSplitterNode {
    /// Output mode
    output_mode: OutputMode,
    /// Maximum speakers to support
    max_speakers: u32,
    /// Stream ID prefix
    stream_id_prefix: String,
    /// Sample rate
    sample_rate: u32,
    /// Per-session state
    states: Arc<Mutex<HashMap<String, SplitterState>>>,
}

impl AudioChannelSplitterNode {
    /// Create with configuration
    pub fn with_config(config: AudioChannelSplitterConfig) -> Self {
        Self {
            output_mode: config.output_mode,
            max_speakers: config.max_speakers,
            stream_id_prefix: config.stream_id_prefix,
            sample_rate: config.sample_rate,
            states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create with default configuration
    pub fn new() -> Self {
        Self::with_config(AudioChannelSplitterConfig::default())
    }

    /// Extract speaker-specific audio based on segments
    fn extract_speaker_audio(
        &self,
        samples: &[f32],
        sample_rate: u32,
        segments: &[DiarizationSegment],
        time_offset: f64,
        speaker_id: &str,
    ) -> Vec<f32> {
        let mut speaker_audio = vec![0.0f32; samples.len()];
        let audio_duration = samples.len() as f64 / sample_rate as f64;

        for segment in segments {
            if segment.speaker != speaker_id {
                continue;
            }

            // Calculate sample indices relative to this audio chunk
            let segment_start_in_chunk = segment.start - time_offset;
            let segment_end_in_chunk = segment.end - time_offset;

            // Skip if segment is completely outside this chunk
            if segment_end_in_chunk < 0.0 || segment_start_in_chunk > audio_duration {
                continue;
            }

            // Clamp to chunk boundaries
            let start_sec = segment_start_in_chunk.max(0.0);
            let end_sec = segment_end_in_chunk.min(audio_duration);

            let start_sample = (start_sec * sample_rate as f64) as usize;
            let end_sample = (end_sec * sample_rate as f64) as usize;

            // Copy audio samples for this speaker's segment
            let start_idx = start_sample.min(samples.len());
            let end_idx = end_sample.min(samples.len());

            for i in start_idx..end_idx {
                speaker_audio[i] = samples[i];
            }
        }

        speaker_audio
    }

    /// Build multi-channel output (interleaved samples)
    fn build_multichannel_audio(
        &self,
        samples: &[f32],
        sample_rate: u32,
        segments: &[DiarizationSegment],
        time_offset: f64,
        num_speakers: usize,
    ) -> Vec<f32> {
        let num_channels = num_speakers.min(self.max_speakers as usize);
        let num_samples = samples.len();
        let mut multichannel = vec![0.0f32; num_samples * num_channels];

        for speaker_idx in 0..num_channels {
            let speaker_id = speaker_idx.to_string();
            let speaker_audio = self.extract_speaker_audio(
                samples,
                sample_rate,
                segments,
                time_offset,
                &speaker_id,
            );

            // Interleave into multichannel output
            for (sample_idx, &sample) in speaker_audio.iter().enumerate() {
                let interleaved_idx = sample_idx * num_channels + speaker_idx;
                if interleaved_idx < multichannel.len() {
                    multichannel[interleaved_idx] = sample;
                }
            }
        }

        multichannel
    }
}

impl Default for AudioChannelSplitterNode {
    fn default() -> Self {
        Self::new()
    }
}

// Phase A-Wave 3: migrated to `SyncStreamingNode` with
// `parking_lot::Mutex`. The helpers never awaited — only the outer
// trait did. Collect-then-fire: state mutation happens under the
// lock, lock is dropped before the callbacks fire (via the existing
// `drop(states)` in the Audio branch).
impl SyncStreamingNode for AudioChannelSplitterNode {
    fn node_type(&self) -> &str {
        "AudioChannelSplitterNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "AudioChannelSplitterNode requires streaming mode - \
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
        let mut states = self.states.lock();
        let state = states.entry(session_key.clone()).or_default();

        match &data {
            // Handle diarization JSON - store segments for later use
            RuntimeData::Json(json) => {
                if let Some(segments_arr) = json.get("segments").and_then(|s| s.as_array()) {
                    state.segments.clear();
                    for seg_val in segments_arr {
                        if let Ok(seg) =
                            serde_json::from_value::<DiarizationSegment>(seg_val.clone())
                        {
                            state.segments.push(seg);
                        }
                    }
                }

                if let Some(num) = json.get("num_speakers").and_then(|n| n.as_u64()) {
                    state.num_speakers = num as usize;
                }

                if let Some(offset) = json.get("time_offset").and_then(|t| t.as_f64()) {
                    state.time_offset = offset;
                }

                tracing::debug!(
                    "Updated diarization state: {} segments, {} speakers, offset {:.2}s",
                    state.segments.len(),
                    state.num_speakers,
                    state.time_offset
                );

                // Don't output the JSON, just store it
                Ok(0)
            }

            // Handle audio - split by speaker
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id: input_stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata: _,
            } => {
                // Validate sample rate matches expected (diarization models expect specific rate)
                if *sample_rate != self.sample_rate {
                    tracing::warn!(
                        "Audio sample rate ({}) differs from expected rate ({}). \
                         Diarization timing may be inaccurate.",
                        sample_rate,
                        self.sample_rate
                    );
                }

                // If no segments yet, just pass through
                if state.segments.is_empty() {
                    tracing::debug!("No diarization segments yet, passing through audio");
                    callback(data)?;
                    return Ok(1);
                }

                let mono_samples: Vec<f32> = if *channels > 1 {
                    samples
                        .chunks(*channels as usize)
                        .map(|chunk| chunk.iter().sum::<f32>() / *channels as f32)
                        .collect()
                } else {
                    samples.to_vec()
                };

                let segments = state.segments.clone();
                let num_speakers = state.num_speakers;
                let time_offset = state.time_offset;

                drop(states); // Release lock before producing output

                let mut outputs = 0;

                match self.output_mode {
                    OutputMode::Streams => {
                        // Output separate stream for each speaker
                        for speaker_idx in 0..num_speakers.min(self.max_speakers as usize) {
                            let speaker_id = speaker_idx.to_string();
                            let speaker_audio = self.extract_speaker_audio(
                                &mono_samples,
                                *sample_rate,
                                &segments,
                                time_offset,
                                &speaker_id,
                            );

                            // Check if this speaker has any non-zero audio
                            let has_audio = speaker_audio.iter().any(|&s| s.abs() > 0.0001);
                            if !has_audio {
                                continue;
                            }

                            let stream_id = format!("{}_{}", self.stream_id_prefix, speaker_idx);

                            callback(RuntimeData::Audio {
                                samples: speaker_audio.into(),
                                sample_rate: *sample_rate,
                                channels: 1, // Mono per speaker
                                stream_id: Some(stream_id),
                                timestamp_us: *timestamp_us,
                                arrival_ts_us: *arrival_ts_us,
                                metadata: None,
                            })?;
                            outputs += 1;
                        }
                    }

                    OutputMode::Channels => {
                        // Output multi-channel audio with each speaker in a channel
                        let multichannel = self.build_multichannel_audio(
                            &mono_samples,
                            *sample_rate,
                            &segments,
                            time_offset,
                            num_speakers,
                        );

                        let num_channels = num_speakers.min(self.max_speakers as usize) as u32;

                        callback(RuntimeData::Audio {
                            samples: multichannel.into(),
                            sample_rate: *sample_rate,
                            channels: num_channels,
                            stream_id: input_stream_id.clone(),
                            timestamp_us: *timestamp_us,
                            arrival_ts_us: *arrival_ts_us,
                            metadata: None,
                        })?;
                        outputs += 1;
                    }
                }

                Ok(outputs)
            }

            // Pass through other data types
            _ => {
                callback(data)?;
                Ok(1)
            }
        }
    }
}

impl AudioChannelSplitterNode {
    /// Check if this node is stateful
    pub fn is_stateful(&self) -> bool {
        true
    }

    /// Reset state for all sessions.
    ///
    /// Phase A-Wave 3: no longer requires `block_in_place` — backing
    /// lock is `parking_lot::Mutex`.
    pub fn reset_state(&mut self) {
        self.states.lock().clear();
        tracing::info!("Audio channel splitter states reset");
    }
}

/// Factory for creating AudioChannelSplitterNode instances
pub struct AudioChannelSplitterNodeFactory;

impl crate::nodes::StreamingNodeFactory for AudioChannelSplitterNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> std::result::Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        let config: AudioChannelSplitterConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();

        let node = AudioChannelSplitterNode::with_config(config);
        Ok(Box::new(crate::nodes::SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "AudioChannelSplitterNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AudioChannelSplitterNode")
                .description("Routes audio by speaker to separate streams or channels")
                .category("audio")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Json])
                .produces([RuntimeDataType::Audio])
                .config_schema_from::<AudioChannelSplitterConfig>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = AudioChannelSplitterConfig::default();
        assert_eq!(config.output_mode, OutputMode::Streams);
        assert_eq!(config.max_speakers, 8);
        assert_eq!(config.stream_id_prefix, "speaker");
    }

    #[test]
    fn test_extract_speaker_audio() {
        let node = AudioChannelSplitterNode::new();

        // 1 second of audio at 16kHz
        let samples: Vec<f32> = (0..16000)
            .map(|i| (i as f32 / 16000.0) * 2.0 - 1.0)
            .collect();

        let segments = vec![
            DiarizationSegment {
                start: 0.0,
                end: 0.5,
                speaker: "0".to_string(),
            },
            DiarizationSegment {
                start: 0.5,
                end: 1.0,
                speaker: "1".to_string(),
            },
        ];

        let speaker0_audio = node.extract_speaker_audio(&samples, 16000, &segments, 0.0, "0");
        let speaker1_audio = node.extract_speaker_audio(&samples, 16000, &segments, 0.0, "1");

        // Speaker 0 should have audio in first half, silence in second half
        assert!(speaker0_audio[1000].abs() > 0.01); // Has audio in first half
        assert!(speaker0_audio[12000].abs() < 0.0001); // Silence in second half

        // Speaker 1 should be opposite
        assert!(speaker1_audio[1000].abs() < 0.0001); // Silence in first half
        assert!(speaker1_audio[12000].abs() > 0.01); // Has audio in second half
    }

    #[test]
    fn test_output_mode_serde() {
        let streams: OutputMode = serde_json::from_str("\"streams\"").unwrap();
        assert_eq!(streams, OutputMode::Streams);

        let channels: OutputMode = serde_json::from_str("\"channels\"").unwrap();
        assert_eq!(channels, OutputMode::Channels);
    }
}
