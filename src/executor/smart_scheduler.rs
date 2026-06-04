/// Smart Pipeline Scheduler
///
/// Automatically detects bottlenecks and applies optimizations:
/// - Wraps non-parallelizable nodes with BufferedProcessor
/// - Detects batch-aware nodes and optimizes accordingly
/// - Monitors queue depths and adjusts buffering strategies
/// - Provides real-time metrics and warnings

use crate::data::RuntimeData;
use crate::error::Result;
use crate::manifest::Manifest;
use crate::nodes::{AsyncStreamingNode, StreamingNode};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

/// Node capability detection
#[derive(Debug, Clone)]
pub struct NodeCapabilities {
    /// Can process multiple inputs in parallel
    pub parallelizable: bool,
    
    /// Benefits from batch processing
    pub batch_aware: bool,
    
    /// Typical processing time per item (ms)
    pub avg_processing_time_ms: u64,
    
    /// Maximum throughput (items/second)
    pub max_throughput: f64,
}

/// Queue metrics for monitoring
#[derive(Debug, Clone)]
pub struct QueueMetrics {
    /// Current queue depth
    pub depth: usize,
    
    /// Average wait time in queue (ms)
    pub avg_wait_ms: f64,
    
    /// Maximum wait time observed (ms)
    pub max_wait_ms: u64,
    
    /// Items processed in last second
    pub throughput: usize,
}

/// Smart scheduler that optimizes pipeline execution
pub struct SmartScheduler {
    /// Node capabilities cache
    capabilities: Arc<RwLock<HashMap<String, NodeCapabilities>>>,
    
    /// Queue metrics per node
    queue_metrics: Arc<RwLock<HashMap<String, QueueMetrics>>>,
    
    /// Automatic buffering thresholds
    auto_buffer_threshold: usize,  // Queue depth that triggers buffering
    
    /// Warning thresholds
    latency_warning_ms: u64,
}

impl SmartScheduler {
    pub fn new() -> Self {
        Self {
            capabilities: Arc::new(RwLock::new(HashMap::new())),
            queue_metrics: Arc::new(RwLock::new(HashMap::new())),
            auto_buffer_threshold: 3,  // Auto-buffer if queue > 3
            latency_warning_ms: 100,   // Warn if latency > 100ms
        }
    }
    
    /// Analyze a node and determine its capabilities
    pub async fn analyze_node(
        &self,
        node_id: &str,
        node: &dyn StreamingNode,
    ) -> NodeCapabilities {
        // Check if node type is known to be non-parallelizable
        let non_parallel_types = [
            "KokoroTTSNode",
            "WhisperNode",
            "LFM2AudioNode",
            "OpusEncoderNode",
            "OpusDecoderNode",
        ];
        
        let parallelizable = !non_parallel_types
            .iter()
            .any(|&t| node.node_type() == t);
        
        // Check if node is batch-aware
        let batch_aware_types = [
            "KokoroTTSNode",
            "WhisperNode",
            "TranslationNode",
        ];
        
        let batch_aware = batch_aware_types
            .iter()
            .any(|&t| node.node_type() == t);
        
        // Estimate processing time based on node type
        let avg_processing_time_ms = match node.node_type() {
            "KokoroTTSNode" => 2000,    // TTS is slow
            "WhisperNode" => 1000,      // ASR is medium
            "LFM2AudioNode" => 500,     // LLM is medium-fast
            "SileroVADNode" => 50,      // VAD is fast
            "FastResampleNode" => 10,   // Resampling is very fast
            _ => 100,                   // Default estimate
        };
        
        let max_throughput = 1000.0 / avg_processing_time_ms as f64;
        
        NodeCapabilities {
            parallelizable,
            batch_aware,
            avg_processing_time_ms,
            max_throughput,
        }
    }
    
    /// Wrap node with appropriate optimizations based on capabilities
    pub async fn optimize_node(
        &self,
        node_id: String,
        node: Box<dyn StreamingNode>,
        manifest: &Manifest,
    ) -> Box<dyn StreamingNode> {
        // Get or analyze capabilities
        let capabilities = {
            let caps_lock = self.capabilities.read().await;
            if let Some(caps) = caps_lock.get(&node_id) {
                caps.clone()
            } else {
                drop(caps_lock);
                let caps = self.analyze_node(&node_id, node.as_ref()).await;
                let mut caps_lock = self.capabilities.write().await;
                caps_lock.insert(node_id.clone(), caps.clone());
                caps
            }
        };
        
        // Apply optimizations based on capabilities
        if !capabilities.parallelizable {
            tracing::info!(
                "[SmartScheduler] Node {} is non-parallelizable, adding BufferedProcessor",
                node_id
            );
            
            // Determine merge strategy based on node connections
            let merge_strategy = self.determine_merge_strategy(&node_id, manifest);
            
            // Wrap with BufferedProcessor
            use crate::nodes::buffered_processor::{BufferedProcessor, MergeStrategy};
            
            let async_node = Arc::new(AsyncNodeAdapter::new(node));
            let buffered = BufferedProcessor::new(async_node, merge_strategy)
                .with_config(
                    10,  // max buffer size
                    capabilities.avg_processing_time_ms / 10,  // max wait = 10% of processing time
                    if capabilities.batch_aware { 3 } else { 1 },  // min batch size
                );
            
            Box::new(StreamingNodeAdapter::new(Box::new(buffered)))
        } else {
            node
        }
    }
    
    /// Determine the best merge strategy for a node
    fn determine_merge_strategy(
        &self,
        node_id: &str,
        manifest: &Manifest,
    ) -> crate::nodes::buffered_processor::MergeStrategy {
        use crate::nodes::buffered_processor::MergeStrategy;
        
        // Find what type of data this node processes
        if let Some(node_spec) = manifest.nodes.iter().find(|n| n.id == node_id) {
            match node_spec.node_type.as_str() {
                "KokoroTTSNode" | "TranslationNode" => {
                    // Text nodes: concatenate with space
                    MergeStrategy::ConcatenateText {
                        separator: " ".to_string(),
                    }
                }
                "OpusEncoderNode" | "OpusDecoderNode" => {
                    // Audio nodes: concatenate samples
                    MergeStrategy::ConcatenateAudio
                }
                _ => MergeStrategy::KeepSeparate,
            }
        } else {
            MergeStrategy::KeepSeparate
        }
    }
    
    /// Monitor queue depth and warn about bottlenecks
    pub async fn monitor_queue(
        &self,
        node_id: &str,
        queue_depth: usize,
        avg_wait_ms: f64,
    ) {
        let mut metrics = self.queue_metrics.write().await;
        let metric = metrics.entry(node_id.to_string()).or_insert(QueueMetrics {
            depth: 0,
            avg_wait_ms: 0.0,
            max_wait_ms: 0,
            throughput: 0,
        });
        
        metric.depth = queue_depth;
        metric.avg_wait_ms = avg_wait_ms;
        
        if queue_depth > self.auto_buffer_threshold {
            tracing::warn!(
                "[SmartScheduler] Node {} has queue depth {}, consider enabling buffering",
                node_id,
                queue_depth
            );
        }
        
        if avg_wait_ms > self.latency_warning_ms as f64 {
            tracing::warn!(
                "[SmartScheduler] Node {} has high latency: {:.2}ms average wait time",
                node_id,
                avg_wait_ms
            );
        }
    }
    
    /// Get optimization recommendations for the pipeline
    pub async fn get_recommendations(&self, manifest: &Manifest) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        let capabilities = self.capabilities.read().await;
        let metrics = self.queue_metrics.read().await;
        
        for node in &manifest.nodes {
            if let Some(caps) = capabilities.get(&node.id) {
                // Check for non-parallelizable bottlenecks
                if !caps.parallelizable {
                    if let Some(metric) = metrics.get(&node.id) {
                        if metric.depth > 2 {
                            recommendations.push(format!(
                                "Node {} is a bottleneck with queue depth {}. Consider: \
                                1) Enable batch processing if supported, \
                                2) Add multiple instances for load balancing, \
                                3) Optimize the node implementation",
                                node.id, metric.depth
                            ));
                        }
                    }
                }
                
                // Check for batch-aware nodes not using batching
                if caps.batch_aware {
                    recommendations.push(format!(
                        "Node {} supports batch processing. \
                        Enable BufferedProcessor with batch size 3-5 for better throughput",
                        node.id
                    ));
                }
            }
        }
        
        recommendations
    }
}

/// Adapter to make AsyncStreamingNode work with StreamingNode interface
struct AsyncNodeAdapter {
    inner: Box<dyn StreamingNode>,
}

impl AsyncNodeAdapter {
    fn new(inner: Box<dyn StreamingNode>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for AsyncNodeAdapter {
    fn node_type(&self) -> &str {
        self.inner.node_type()
    }
    
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        // Block on the sync version
        tokio::task::block_in_place(|| {
            futures::executor::block_on(async {
                self.inner.process(data).await
            })
        })
    }
}

/// Adapter to make BufferedProcessor work with StreamingNode interface
struct StreamingNodeAdapter {
    inner: Box<dyn AsyncStreamingNode>,
}

impl StreamingNodeAdapter {
    fn new(inner: Box<dyn AsyncStreamingNode>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl StreamingNode for StreamingNodeAdapter {
    fn node_type(&self) -> &str {
        self.inner.node_type()
    }
    
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        self.inner.process(data).await
    }
    
    fn is_stateful(&self) -> bool {
        true  // BufferedProcessor is stateful
    }
}




