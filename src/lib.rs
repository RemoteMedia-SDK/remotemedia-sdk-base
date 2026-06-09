//! RemoteMedia Runtime Core - Transport-agnostic execution engine
//!
//! This crate provides the core runtime functionality for executing RemoteMedia
//! pipelines without any transport-specific dependencies.
//!
//! # Architecture
//!
//! Runtime-core is a pure library that:
//! - Defines transport abstractions (`PipelineTransport`, `StreamSession` traits)
//! - Provides execution engine (`PipelineExecutor`)
//! - Manages pipeline graphs, node execution, and session routing
//! - Has ZERO dependencies on transport crates (no tonic, prost, pyo3, etc.)
//!
//! Transport implementations (gRPC, FFI, WebRTC) are separate crates that:
//! - Depend on `remotemedia-core`
//! - Implement the `PipelineTransport` trait
//! - Handle their own serialization formats
//!
//! # Example
//!
//! ```
//! use remotemedia_core::transport::PipelineExecutor;
//! use remotemedia_core::transport::TransportData;
//! use remotemedia_core::data::RuntimeData;
//!
//! // Create the pipeline executor
//! let executor = PipelineExecutor::new().unwrap();
//!
//! // Create transport data
//! let input = TransportData::new(RuntimeData::Text("hello".into()));
//!
//! // Use executor.execute_unary(manifest, input).await for execution
//! ```

#![warn(clippy::all)]
#![allow(clippy::arc_with_non_send_sync)] // iceoryx2 types are intentionally !Send

// Allow the crate to refer to itself as `remotemedia_core` and `remotemedia_sdk_base` for proc-macro compatibility
extern crate self as remotemedia_core;
extern crate self as remotemedia_sdk_base;

// Core execution modules
pub mod audio;
pub mod capabilities;
pub mod executor;
pub mod ingestion;
pub mod llm;
pub mod metrics;
// `multiprocess::data_transfer` is the binary wire format used by
// both the multiprocess runtime AND the in-process loadable runtime.
// The iceoryx2-using parts inside the module are individually
// feature-gated.
#[cfg(feature = "loadable")]
pub mod loadable;
pub mod multiprocess;
pub mod nodes;
pub mod python;
pub mod validation;
/// Public entrypoint for ergonomic registration macros.
pub mod registration_macros {
    pub use crate::{
        register_python_node, register_python_nodes, register_rust_node, register_rust_node_default,
    };
}

// Manifest
pub use manifest::Manifest;

// Validation - convenience re-exports for introspection API
pub use validation::{get_all_schemas, get_node_schema, SchemaValidator, ValidationResult};

// Transport abstraction layer
pub mod transport;

// Re-export core modules from existing runtime
// NOTE: For Phase 2, these are stub re-exports
// In later phases, we'll copy the actual implementations from runtime/

/// Data types module - transport-agnostic data representations
pub mod data {
    //! Core data types
    //!
    //! Wire-format types ([`RuntimeData`], [`AudioSamples`],
    //! [`PixelFormat`], [`VideoCodec`], [`ImageFormat`],
    //! [`ControlMessageType`], [`AudioBuffer`], [`VideoFrame`],
    //! [`TensorBuffer`], [`AudioFormat`], [`DataTypeHint`],
    //! plus the text-channel helpers) are defined in the skinny
    //! `remotemedia-types` crate and re-exported here so existing
    //! `use remotemedia_core::data::RuntimeData` call sites keep
    //! compiling.
    //!
    //! Execution machinery (`AudioBufferPool`, `BufferingPolicy`,
    //! `RingBuffer`, `SpeculativeSegment`, perf types, the richer
    //! `ControlMessage` struct, RGB→YUV converters) stays here in
    //! core because it pulls heavyweight runtime deps that
    //! out-of-tree plugins must not transitively link.

    // Low-latency streaming data structures (spec 007)
    pub mod audio_buffer_pool;
    pub mod audio_samples;
    pub mod buffering_policy;
    pub mod control_message;
    pub mod perf;
    pub mod ring_buffer;
    pub mod speculative_segment;
    pub mod text_channel;

    pub use audio_buffer_pool::{AudioBufferPool, PooledAudioBuf};
    pub use audio_samples::AudioSamples;
    pub use buffering_policy::{BufferingPolicy, MergeStrategy};
    pub use control_message::{ControlMessage, ControlMessageType};
    pub use perf::{LatencyPercentiles, NodeStats as PerfNodeStats, PerfEventKind, PerfSnapshot};
    pub use ring_buffer::RingBuffer;
    pub use speculative_segment::{SegmentStatus, SpeculativeSegment};
    pub use text_channel::{split_text_str, tag_text_str, TEXT_CHANNEL_DEFAULT};

    // Video codec support (spec 012) — re-exports from
    // `remotemedia-types` plus the RGB→YUV converters.
    pub mod video;
    pub use video::{PixelFormat, VideoCodec};

    // Wire-format enums and the `RuntimeData` discriminant —
    // canonical definitions live in `remotemedia-types`.
    pub use remotemedia_types::{
        AudioBuffer, AudioFormat, DataTypeHint, ImageFormat, RuntimeData, TensorBuffer, VideoFrame,
    };
}

#[cfg(feature = "hf-download")]
pub mod model_downloader;
#[cfg(feature = "hf-download")]
pub use model_downloader::HfModelDownloader;

/// Manifest parsing module
pub mod manifest;

// Error types
mod error;
pub use error::{Error, Result};
pub use serde_json;

// Re-export attribute macros (always available since derive is default)
pub use remotemedia_core_derive::node;
pub use remotemedia_core_derive::node_config;

/// Initialize the RemoteMedia runtime core
///
/// This should be called once at startup to initialize logging.
pub fn init() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("RemoteMedia Runtime Core initialized");
    Ok(())
}

// ============================================================================
// Backward Compatibility Re-exports (v0.3 → v0.4 Migration)
// ============================================================================
//
// These re-exports allow existing code to gradually migrate from the old
// monolithic structure to the new decoupled architecture.
//
// Example migration path:
// ```rust
// // Old (v0.3.x):
// use remotemedia_runtime::grpc_service::GrpcServer;
// use remotemedia_runtime::executor::Executor;
//
// // Transitional (v0.4.x with compat):
// use remotemedia_core::executor::Executor;  // Still works
// // BUT GrpcServer now in: use remotemedia_grpc::GrpcServer;
//
// // New (v0.4.x+):
// use remotemedia_core::transport::PipelineExecutor;
// use remotemedia_grpc::GrpcServer;
// ```
//
// These re-exports will be marked deprecated in v0.5 and removed in v1.0.

/// Backward compatibility: Core execution types remain in core
///
/// **Migration Note**: Continue using from `remotemedia_core::executor`
pub mod executor_compat {
    pub use crate::executor::*;
}

/// Backward compatibility: Data types remain in core
///
/// **Migration Note**: Continue using from `remotemedia_core::data`
pub mod data_compat {
    pub use crate::data::*;
}

/// Backward compatibility: Manifest types remain in core
///
/// **Migration Note**: Continue using from `remotemedia_core::manifest`
pub mod manifest_compat {
    pub use crate::manifest::*;
}

/// Backward compatibility: Node types remain in core
///
/// **Migration Note**: Continue using from `remotemedia_core::nodes`
pub mod nodes_compat {
    pub use crate::nodes::*;
}

// NOTE: gRPC-specific types (GrpcServer, StreamingServiceImpl, ExecutionServiceImpl)
// have been moved to the `remotemedia-grpc` crate and are NOT re-exported here.
// Users must update imports:
//   OLD: use remotemedia_runtime::grpc_service::GrpcServer;
//   NEW: use remotemedia_grpc::GrpcServer;

#[cfg(test)]
mod tests {
    use super::*;
    use data::video::{PixelFormat, VideoCodec};

    #[test]
    fn test_init() {
        // Should not panic
        init().ok();
    }

    // T041: Unit tests for RuntimeData::Video validation
    #[test]
    fn test_video_frame_validation_valid() {
        // Valid 720p YUV420P frame
        let frame = data::RuntimeData::Video {
            pixel_data: vec![128u8; 1_382_400], // 1280*720*1.5
            width: 1280,
            height: 720,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        assert!(frame.validate_video_frame().is_ok());
    }

    #[test]
    fn test_video_frame_validation_zero_dimensions() {
        let frame = data::RuntimeData::Video {
            pixel_data: vec![],
            width: 0, // Invalid
            height: 720,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let result = frame.validate_video_frame();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("width and height must be > 0"));
    }

    #[test]
    fn test_video_frame_validation_odd_dimensions_yuv() {
        // YUV formats require even dimensions
        let frame = data::RuntimeData::Video {
            pixel_data: vec![128u8; 100],
            width: 1281, // Odd width
            height: 720,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let result = frame.validate_video_frame();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("even dimensions"));
    }

    #[test]
    fn test_video_frame_validation_buffer_size_mismatch() {
        // Buffer size doesn't match format
        let frame = data::RuntimeData::Video {
            pixel_data: vec![128u8; 1000], // Wrong size for 1280x720 YUV420P
            width: 1280,
            height: 720,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let result = frame.validate_video_frame();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Buffer size mismatch"));
    }

    #[test]
    fn test_video_frame_validation_rgb24() {
        // Valid RGB24 frame (odd dimensions OK)
        let frame = data::RuntimeData::Video {
            pixel_data: vec![0u8; 1920 * 1081 * 3], // Odd height OK for RGB
            width: 1920,
            height: 1081, // Odd height
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        assert!(frame.validate_video_frame().is_ok());
    }

    #[test]
    fn test_video_frame_validation_encoded_variable_size() {
        // Encoded frames have variable size (validation skipped)
        let frame = data::RuntimeData::Video {
            pixel_data: vec![0u8; 5000], // Variable encoded size
            width: 1280,
            height: 720,
            format: PixelFormat::Encoded,
            codec: Some(VideoCodec::Vp8),
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        };

        assert!(frame.validate_video_frame().is_ok());
    }

    // spec 026: Tests for RuntimeData timing methods
    #[test]
    fn test_runtime_data_timing_audio() {
        let audio = data::RuntimeData::Audio {
            samples: vec![0.0; 100].into(),
            sample_rate: 44100,
            channels: 1,
            stream_id: Some("audio_main".to_string()),
            timestamp_us: Some(1_000_000),
            arrival_ts_us: Some(1_001_000),
            metadata: None,
        };

        let (media_ts, arrival_ts) = audio.timing();
        assert_eq!(media_ts, Some(1_000_000));
        assert_eq!(arrival_ts, Some(1_001_000));
        assert_eq!(audio.stream_id(), Some("audio_main"));
        assert!(audio.is_audio());
        assert!(!audio.is_video());
        assert!(audio.is_timed_media());
    }

    #[test]
    fn test_runtime_data_timing_video() {
        let video = data::RuntimeData::Video {
            pixel_data: vec![0u8; 1000],
            width: 100,
            height: 100,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 0,
            timestamp_us: 2_000_000,
            is_keyframe: true,
            stream_id: Some("video_main".to_string()),
            arrival_ts_us: Some(2_001_000),
        };

        let (media_ts, arrival_ts) = video.timing();
        assert_eq!(media_ts, Some(2_000_000));
        assert_eq!(arrival_ts, Some(2_001_000));
        assert_eq!(video.stream_id(), Some("video_main"));
        assert!(!video.is_audio());
        assert!(video.is_video());
        assert!(video.is_timed_media());
    }

    #[test]
    fn test_runtime_data_timing_non_media() {
        let text = data::RuntimeData::Text("hello".to_string());

        let (media_ts, arrival_ts) = text.timing();
        assert_eq!(media_ts, None);
        assert_eq!(arrival_ts, None);
        assert_eq!(text.stream_id(), None);
        assert!(!text.is_audio());
        assert!(!text.is_video());
        assert!(!text.is_timed_media());
    }

    #[test]
    fn test_runtime_data_set_timestamps() {
        let mut audio = data::RuntimeData::Audio {
            samples: vec![0.0; 100].into(),
            sample_rate: 44100,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        // Set arrival timestamp
        assert!(audio.set_arrival_timestamp(5_000_000));
        let (_, arrival_ts) = audio.timing();
        assert_eq!(arrival_ts, Some(5_000_000));

        // Set media timestamp
        assert!(audio.set_audio_timestamp(4_000_000));
        let (media_ts, _) = audio.timing();
        assert_eq!(media_ts, Some(4_000_000));

        // Non-audio types should return false
        let mut text = data::RuntimeData::Text("hello".to_string());
        assert!(!text.set_arrival_timestamp(1000));
        assert!(!text.set_audio_timestamp(1000));
    }

    // T010-T013: Unit tests for RuntimeData::File (spec 001)
    #[test]
    fn test_file_data_type() {
        let file = data::RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(104_857_600),
            offset: None,
            length: None,
            stream_id: None,
        };

        assert_eq!(file.data_type(), "file");
    }

    #[test]
    fn test_file_item_count() {
        let file = data::RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: None,
            mime_type: None,
            size: None,
            offset: None,
            length: None,
            stream_id: None,
        };

        assert_eq!(file.item_count(), 1);
    }

    #[test]
    fn test_file_size_bytes() {
        let file = data::RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(104_857_600),
            offset: None,
            length: None,
            stream_id: Some("main".to_string()),
        };

        // path(21) + filename(9) + mime_type(9) + stream_id(4) + 24 (3 u64s) = 67
        assert_eq!(file.size_bytes(), 67);
    }

    #[test]
    fn test_file_with_all_fields() {
        let file = data::RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(104_857_600),
            offset: Some(1024 * 1024), // 1 MB offset
            length: Some(64 * 1024),   // 64 KB chunk
            stream_id: Some("video_track".to_string()),
        };

        assert_eq!(file.data_type(), "file");
        assert_eq!(file.item_count(), 1);
    }

    #[test]
    fn test_file_with_only_path() {
        // Minimal file reference with only required field
        let file = data::RuntimeData::File {
            path: "/tmp/output.bin".to_string(),
            filename: None,
            mime_type: None,
            size: None,
            offset: None,
            length: None,
            stream_id: None,
        };

        assert_eq!(file.data_type(), "file");
        assert_eq!(file.item_count(), 1);
        // path(15) + 24 (3 u64s) = 39
        assert_eq!(file.size_bytes(), 39);
    }

    #[test]
    fn test_file_serde_serialization() {
        let file = data::RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(104_857_600),
            offset: None,
            length: None,
            stream_id: None,
        };

        // Test serialization
        let json = serde_json::to_string(&file).unwrap();
        assert!(json.contains("File"));
        assert!(json.contains("/data/input/video.mp4"));
        assert!(json.contains("video.mp4"));
        assert!(json.contains("video/mp4"));
        assert!(json.contains("104857600"));

        // Test deserialization roundtrip
        let deserialized: data::RuntimeData = serde_json::from_str(&json).unwrap();
        assert_eq!(file, deserialized);
    }

    #[test]
    fn test_file_serde_skip_none_fields() {
        // File with minimal fields should have compact serialization
        let file = data::RuntimeData::File {
            path: "/tmp/test.txt".to_string(),
            filename: None,
            mime_type: None,
            size: None,
            offset: None,
            length: None,
            stream_id: None,
        };

        let json = serde_json::to_string(&file).unwrap();
        // None fields should be omitted due to skip_serializing_if
        assert!(!json.contains("filename"));
        assert!(!json.contains("mime_type"));
        assert!(!json.contains("offset"));
        assert!(!json.contains("length"));
        assert!(!json.contains("stream_id"));

        // Roundtrip should still work
        let deserialized: data::RuntimeData = serde_json::from_str(&json).unwrap();
        assert_eq!(file, deserialized);
    }

    #[test]
    fn test_file_byte_range_fields() {
        // Test byte range request
        let range_request = data::RuntimeData::File {
            path: "/data/large_file.bin".to_string(),
            filename: None,
            mime_type: None,
            size: Some(1_073_741_824),      // 1 GB
            offset: Some(10 * 1024 * 1024), // 10 MB offset
            length: Some(64 * 1024),        // 64 KB chunk
            stream_id: None,
        };

        assert_eq!(range_request.data_type(), "file");

        // Verify serialization includes offset and length
        let json = serde_json::to_string(&range_request).unwrap();
        assert!(json.contains("10485760")); // offset
        assert!(json.contains("65536")); // length
    }

    #[test]
    fn test_data_type_hint_file() {
        assert_eq!(data::DataTypeHint::File as i32, 8);
    }
}

// ============================================================================
// Pipeline Host Facade
// ============================================================================

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Consolidated public entry point for local pipeline execution.
pub struct PipelineHost {
    executor: Arc<transport::PipelineExecutor>,
    manifest: Arc<Manifest>,
    #[allow(dead_code)]
    prewarm_pool: Option<Arc<transport::warm_session_pool::WarmSessionPool>>,
}

impl PipelineHost {
    /// Start building a PipelineHost from a YAML manifest file.
    pub fn from_manifest_file<P: AsRef<Path>>(path: P) -> PipelineHostBuilder {
        PipelineHostBuilder::new(path)
    }

    /// Create a new session for the pipeline manifest.
    pub async fn create_session(&self) -> Result<transport::SessionHandle> {
        self.executor.create_session(self.manifest.clone()).await
    }

    /// Access the underlying PipelineExecutor.
    pub fn executor(&self) -> Arc<transport::PipelineExecutor> {
        self.executor.clone()
    }

    /// Access the underlying Manifest.
    pub fn manifest(&self) -> Arc<Manifest> {
        self.manifest.clone()
    }
}

impl transport::PipelineSessionHost for PipelineHost {
    fn create_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, Result<transport::SessionHandle>> {
        let executor = self.executor.clone();
        Box::pin(async move { executor.create_session(manifest).await })
    }

    fn get_or_create_shared_session(
        &self,
        key: String,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, Result<Arc<transport::shared_session::SharedPipelineSession>>>
    {
        let executor = self.executor.clone();
        Box::pin(async move { executor.get_or_create_shared_session(key, manifest).await })
    }

    fn control_bus(&self) -> Arc<transport::session_control::SessionControlBus> {
        self.executor.control_bus()
    }

    fn registry(&self) -> Arc<RwLock<nodes::StreamingNodeRegistry>> {
        self.executor.registry().clone()
    }
}

/// Fluent builder for PipelineHost instances.
pub struct PipelineHostBuilder {
    manifest_path: PathBuf,
    load_plugins: bool,
    prewarm_count: Option<usize>,
}

impl PipelineHostBuilder {
    /// Create a new builder with the path to the pipeline manifest file.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            manifest_path: path.as_ref().to_path_buf(),
            load_plugins: false,
            prewarm_count: None,
        }
    }

    /// Configure the host to pre-load any plugins declared in the manifest.
    pub fn with_plugins_from_manifest(mut self) -> Self {
        self.load_plugins = true;
        self
    }

    /// Configure local session prewarming with the specified target count.
    pub fn with_local_prewarm(mut self, count: usize) -> Self {
        self.prewarm_count = Some(count);
        self
    }

    /// Build and initialize the PipelineHost instance.
    pub async fn build(self) -> anyhow::Result<PipelineHost> {
        use std::path::Path;
        // Read and parse manifest
        let manifest_content = tokio::fs::read_to_string(&self.manifest_path)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read manifest file at {:?}: {}",
                    self.manifest_path,
                    e
                )
            })?;

        let manifest: Manifest = serde_yaml::from_str(&manifest_content).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse YAML manifest from {:?}: {}",
                self.manifest_path,
                e
            )
        })?;
        let manifest = Arc::new(manifest);

        // Configure executor with the parent directory of the manifest
        // so relative plugin paths resolve relative to the manifest file.
        let manifest_base_dir = self.manifest_path.parent().map(|p| p.to_path_buf());
        let mut config = transport::ExecutorConfig::default();
        config.manifest_base_dir = manifest_base_dir;

        let executor = transport::PipelineExecutor::with_config(config)
            .map_err(|e| anyhow::anyhow!("Failed to construct executor: {:?}", e))?;
        let executor = Arc::new(executor);

        // Pre-load plugins if requested
        if self.load_plugins {
            executor
                .ensure_plugins_loaded(&manifest)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to pre-load plugins: {:?}", e))?;
        }

        // Configure session prewarming if requested
        let mut prewarm_pool = None;
        if let Some(count) = self.prewarm_count {
            if count > 0 {
                let pool = transport::warm_session_pool::WarmSessionPool::new(executor.clone());
                executor.set_default_pool(pool.clone());
                pool.set_target(manifest.clone(), count).await;
                prewarm_pool = Some(pool);
            }
        }

        Ok(PipelineHost {
            executor,
            manifest,
            prewarm_pool,
        })
    }
}
