/// Batch-Aware Node Interface
///
/// Nodes that implement this trait can process multiple inputs more efficiently
/// as a batch than processing them individually.
///
/// Example: TTS can synthesize "Hello. How are you? I'm fine." faster as one
/// request than three separate requests.

use crate::data::RuntimeData;
use crate::error::Result;
use async_trait::async_trait;

/// Trait for nodes that can process batches more efficiently
#[async_trait]
pub trait BatchAwareNode: Send + Sync {
    /// Process multiple inputs as a batch
    /// Returns results in the same order as inputs
    async fn process_batch(&self, inputs: Vec<RuntimeData>) -> Result<Vec<RuntimeData>>;
    
    /// Get optimal batch size for this node
    fn optimal_batch_size(&self) -> usize {
        5  // Default: process 5 items at a time
    }
    
    /// Maximum time to wait for a batch to fill (ms)
    fn max_batch_wait_ms(&self) -> u64 {
        100  // Default: wait up to 100ms for batch to fill
    }
    
    /// Whether this node benefits from batching
    fn supports_batching(&self) -> bool {
        true
    }
}

/// Wrapper that makes any TTS node batch-aware
pub struct BatchAwareTTS {
    tts_processor: Box<dyn Fn(String) -> Vec<f32> + Send + Sync>,
}

impl BatchAwareTTS {
    pub fn new<F>(processor: F) -> Self 
    where
        F: Fn(String) -> Vec<f32> + Send + Sync + 'static,
    {
        Self {
            tts_processor: Box::new(processor),
        }
    }
}

#[async_trait]
impl BatchAwareNode for BatchAwareTTS {
    async fn process_batch(&self, inputs: Vec<RuntimeData>) -> Result<Vec<RuntimeData>> {
        // Extract all text inputs
        let texts: Vec<String> = inputs
            .into_iter()
            .filter_map(|data| match data {
                RuntimeData::Text(s) => Some(s),
                _ => None,
            })
            .collect();
        
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        
        // Merge all texts with proper spacing
        let merged_text = texts.join(" ");
        
        tracing::info!(
            "[BatchAwareTTS] Processing batch of {} sentences as single request",
            texts.len()
        );
        
        // Process as single TTS request
        let audio_samples = (self.tts_processor)(merged_text);
        
        // Return single audio output
        Ok(vec![RuntimeData::Audio {
            samples: audio_samples,
            sample_rate: 24000,
            channels: 1,
        }])
    }
    
    fn optimal_batch_size(&self) -> usize {
        3  // TTS works well with 3-5 sentences at a time
    }
    
    fn max_batch_wait_ms(&self) -> u64 {
        150  // Wait up to 150ms for sentences to accumulate
    }
}




