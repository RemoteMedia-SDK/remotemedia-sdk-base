//! Pluggable Ingestion Framework
//!
//! This module provides a pluggable abstraction for ingesting media from various
//! sources (files, RTMP streams, SRT, WebRTC, etc.) into RemoteMedia pipelines.
//!
//! # Architecture
//!
//! The ingestion system consists of three main components:
//!
//! 1. **[`IngestPlugin`]** - Factory for creating ingest sources. Handles URI scheme
//!    matching and source validation.
//!
//! 2. **[`IngestSource`]** - Active connection to a media source. Manages connection
//!    lifecycle and produces decoded media data.
//!
//! 3. **[`IngestStream`]** - Async stream of `RuntimeData` chunks from a connected
//!    source.
//!
//! # Usage
//!
//! ## Using the Global Registry
//!
//! The simplest way to use ingestion is through the global registry:
//!
//! ```ignore
//! use remotemedia_core::ingestion::{global_ingest_registry, IngestConfig};
//!
//! // Create config from URI
//! let config = IngestConfig::from_url("./test.wav");
//!
//! // Create source from registry
//! let registry = global_ingest_registry();
//! let mut source = registry.create_from_uri(&config)?;
//!
//! // Start receiving data
//! let mut stream = source.start().await?;
//! while let Some(data) = stream.recv().await {
//!     println!("Received: {:?}", data.data_type());
//! }
//! ```
//!
//! ## Implementing a Custom Plugin
//!
//! To add support for a new protocol:
//!
//! ```ignore
//! use remotemedia_core::ingestion::{IngestPlugin, IngestSource, IngestConfig};
//!
//! pub struct MyProtocolPlugin;
//!
//! impl IngestPlugin for MyProtocolPlugin {
//!     fn name(&self) -> &'static str { "my_protocol" }
//!     
//!     fn schemes(&self) -> &'static [&'static str] { &["myproto"] }
//!     
//!     fn create(&self, config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error> {
//!         Ok(Box::new(MyProtocolSource::new(config)?))
//!     }
//!
//!     fn validate(&self, config: &IngestConfig) -> Result<(), Error> {
//!         // Validate the URL format
//!         Ok(())
//!     }
//! }
//!
//! // Register with the global registry
//! global_ingest_registry().register(Arc::new(MyProtocolPlugin))?;
//! ```
//!
//! # Multi-Track Support
//!
//! Sources with multiple audio/video tracks (e.g., MKV files, RTMP streams) produce
//! `RuntimeData` chunks tagged with `stream_id` values:
//!
//! - `"audio:0"` - First audio track
//! - `"audio:1"` - Second audio track
//! - `"video:0"` - First video track
//! - `"subtitle:0"` - First subtitle track
//!
//! Use [`IngestConfig::track_selection`] to control which tracks are ingested.
//!
//! # Built-in Plugins
//!
//! The following plugins are registered by default:
//!
//! | Plugin | Schemes | Description |
//! |--------|---------|-------------|
//! | `file` | `file://`, bare paths, `-` | Local files and stdin |
//!
//! Additional plugins can be added via adapter crates:
//!
//! | Crate | Schemes | Description |
//! |-------|---------|-------------|
//! | `remotemedia-ingest-rtmp` | `rtmp://`, `rtmps://` | RTMP/RTMPS streams |
//! | `remotemedia-ingest-srt` | `srt://` | SRT streams |
//!

use async_trait::async_trait;

use crate::Error;

// Sub-modules
pub mod config;
pub mod file;
pub mod registry;
pub mod status;
pub mod stream;

// Re-export all public types
pub use config::{
    AudioConfig, IngestConfig, ReconnectConfig, TrackSelection, TrackSelector, VideoConfig,
};
pub use file::{url_to_path, FileIngestPlugin, FileIngestSource};
pub use registry::{global_ingest_registry, IngestRegistry};
pub use status::{IngestStatus, MediaType};
pub use stream::{
    AudioTrackProperties, DataTrackProperties, IngestMetadata, IngestStream,
    SubtitleTrackProperties, TrackInfo, TrackProperties, VideoTrackProperties,
};

/// Factory for creating ingest sources
///
/// Plugins are registered with the [`IngestRegistry`] and matched against
/// URI schemes to create appropriate [`IngestSource`] instances.
///
/// # Implementing a Plugin
///
/// ```ignore
/// use remotemedia_core::ingestion::{IngestPlugin, IngestSource, IngestConfig};
///
/// pub struct RtmpIngestPlugin;
///
/// impl IngestPlugin for RtmpIngestPlugin {
///     fn name(&self) -> &'static str { "rtmp" }
///     
///     fn schemes(&self) -> &'static [&'static str] { &["rtmp", "rtmps"] }
///     
///     fn create(&self, config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error> {
///         Ok(Box::new(RtmpIngestSource::new(config)?))
///     }
/// }
/// ```
pub trait IngestPlugin: Send + Sync {
    /// Plugin name (e.g., "file", "rtmp", "srt")
    ///
    /// Must be unique across all registered plugins.
    fn name(&self) -> &'static str;

    /// URI schemes this plugin handles (e.g., ["file", ""], ["rtmp", "rtmps"])
    ///
    /// Empty string `""` matches bare paths without a scheme (e.g., `./local.wav`).
    /// Special value `"-"` matches stdin.
    fn schemes(&self) -> &'static [&'static str];

    /// Create an ingest source from configuration
    ///
    /// The returned source is in [`IngestStatus::Idle`] state.
    /// Call [`IngestSource::start()`] to begin ingesting.
    fn create(&self, config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error>;

    /// Validate configuration before creation
    ///
    /// Override to add protocol-specific validation (URL format, required fields, etc.)
    /// Default implementation accepts all configs.
    fn validate(&self, config: &IngestConfig) -> Result<(), Error> {
        let _ = config;
        Ok(())
    }
}

/// Active connection to a media source
///
/// Manages the lifecycle of a connection to a media source (file, stream, etc.)
/// and produces decoded media data as [`RuntimeData`] chunks.
///
/// # Lifecycle
///
/// 1. Created via [`IngestPlugin::create()`] in `Idle` state
/// 2. [`start()`] connects and returns an [`IngestStream`]
/// 3. Receive data via [`IngestStream::recv()`]
/// 4. [`stop()`] cleanly disconnects
///
/// # Example
///
/// ```ignore
/// // Create source (in Idle state)
/// let mut source = plugin.create(&config)?;
/// assert_eq!(source.status(), IngestStatus::Idle);
///
/// // Start ingesting (transitions to Connecting, then Connected)
/// let mut stream = source.start().await?;
/// assert_eq!(source.status(), IngestStatus::Connected);
///
/// // Receive data
/// while let Some(data) = stream.recv().await {
///     process(data);
/// }
///
/// // Clean shutdown
/// source.stop().await?;
/// assert_eq!(source.status(), IngestStatus::Disconnected);
/// ```
#[async_trait]
pub trait IngestSource: Send + Sync {
    /// Start ingesting media
    ///
    /// Connects to the source and begins producing [`RuntimeData`] chunks.
    /// Returns an [`IngestStream`] for receiving the data.
    ///
    /// # State Transitions
    ///
    /// - `Idle` → `Connecting` → `Connected`
    /// - On failure: `Idle` → `Connecting` → `Error`
    async fn start(&mut self) -> Result<IngestStream, Error>;

    /// Stop ingesting gracefully
    ///
    /// Signals the source to disconnect cleanly. Any pending data may
    /// still be delivered before the stream closes.
    ///
    /// # State Transitions
    ///
    /// - `Connected` → `Disconnected`
    /// - `Reconnecting` → `Disconnected`
    async fn stop(&mut self) -> Result<(), Error>;

    /// Get current connection status
    fn status(&self) -> IngestStatus;

    /// Get discovered stream metadata
    ///
    /// Returns `None` until the source has connected and discovered
    /// the stream properties (tracks, format, duration).
    fn metadata(&self) -> Option<&IngestMetadata>;
}
