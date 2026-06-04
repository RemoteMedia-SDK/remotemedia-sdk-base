/// Low-latency Audio Buffer Accumulator with pre-buffering
/// 
/// Key improvements:
/// - Pre-buffers audio BEFORE VAD decision
/// - Supports speculative forwarding
/// - Maintains sliding window for context
use crate::data::RuntimeData;
use crate::error::Result;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_PREBUFFER_MS: u32 = 200;  // Pre-buffer window
const DEFAULT_LOOKAHEAD_MS: u32 = 50;   // Speculative lookahead

#[derive(Debug, Clone)]
struct AudioFrame {
    samples: Vec<f32>,
    timestamp_ms: u64,
    sample_rate: u32,
    vad_state: Option<bool>,  // None = pending VAD decision
}

pub struct LowLatencyAudioBuffer {
    /// Sliding window of audio frames
    sliding_window: Arc<Mutex<VecDeque<AudioFrame>>>,
    
    /// Maximum window size in ms
    window_duration_ms: u32,
    
    /// Pre-buffer duration before VAD decision
    prebuffer_ms: u32,
    
    /// Lookahead for speculative processing
    lookahead_ms: u32,
    
    /// Current timestamp
    current_timestamp: Arc<Mutex<u64>>,
    
    /// Sample rate
    sample_rate: u32,
}

impl LowLatencyAudioBuffer {
    pub fn new(
        window_duration_ms: Option<u32>,
        prebuffer_ms: Option<u32>,
        lookahead_ms: Option<u32>,
    ) -> Self {
        Self {
            sliding_window: Arc::new(Mutex::new(VecDeque::new())),
            window_duration_ms: window_duration_ms.unwrap_or(500),
            prebuffer_ms: prebuffer_ms.unwrap_or(DEFAULT_PREBUFFER_MS),
            lookahead_ms: lookahead_ms.unwrap_or(DEFAULT_LOOKAHEAD_MS),
            current_timestamp: Arc::new(Mutex::new(0)),
            sample_rate: 16000,  // Default, will be updated
        }
    }
    
    async fn process_audio_frame(&self, samples: Vec<f32>, sample_rate: u32) -> Result<Option<RuntimeData>> {
        let mut window = self.sliding_window.lock().await;
        let mut timestamp = self.current_timestamp.lock().await;
        
        // Create new frame
        let frame = AudioFrame {
            samples: samples.clone(),
            timestamp_ms: *timestamp,
            sample_rate,
            vad_state: None,  // VAD decision pending
        };
        
        // Add to sliding window
        window.push_back(frame);
        
        // Update timestamp
        let duration_ms = (samples.len() as f32 / sample_rate as f32 * 1000.0) as u64;
        *timestamp += duration_ms;
        
        // Evict old frames beyond window duration
        while !window.is_empty() {
            if let Some(front) = window.front() {
                if *timestamp - front.timestamp_ms > self.window_duration_ms as u64 {
                    window.pop_front();
                } else {
                    break;
                }
            }
        }
        
        // Speculatively forward audio with metadata
        Ok(Some(RuntimeData::Audio {
            samples,
            sample_rate,
            channels: 1,
        }))
    }
    
    async fn apply_vad_decision(&self, timestamp_ms: u64, is_speech: bool) -> Result<()> {
        let mut window = self.sliding_window.lock().await;
        
        // Apply VAD decision retroactively to buffered frames
        for frame in window.iter_mut() {
            if frame.timestamp_ms >= timestamp_ms - self.prebuffer_ms as u64
                && frame.timestamp_ms <= timestamp_ms {
                frame.vad_state = Some(is_speech);
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl AsyncStreamingNode for LowLatencyAudioBuffer {
    fn node_type(&self) -> &str {
        "LowLatencyAudioBuffer"
    }
    
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        match data {
            RuntimeData::Audio { samples, sample_rate, .. } => {
                // Process and immediately forward
                if let Some(output) = self.process_audio_frame(samples, sample_rate).await? {
                    Ok(output)
                } else {
                    Ok(RuntimeData::Empty)
                }
            }
            RuntimeData::Json(vad_event) => {
                // Apply VAD decision retroactively
                if let Some(timestamp) = vad_event.get("timestamp_ms").and_then(|v| v.as_u64()) {
                    if let Some(is_speech) = vad_event.get("has_speech").and_then(|v| v.as_bool()) {
                        self.apply_vad_decision(timestamp, is_speech).await?;
                    }
                }
                Ok(RuntimeData::Empty)
            }
            _ => Ok(data),
        }
    }
}




