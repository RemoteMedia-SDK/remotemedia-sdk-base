//! Fast audio node trait with direct buffer processing (no JSON overhead)

use crate::audio::AudioData;
use crate::error::Result;

/// High-performance audio processing trait
///
/// This trait provides a zero-copy interface for audio processing without
/// JSON serialization overhead. Nodes implementing this trait are 10-15x
/// faster than the standard NodeExecutor trait.
pub trait FastAudioNode: Send {
    /// Process audio data directly (no JSON serialization)
    ///
    /// # Arguments
    /// * `input` - Audio data with buffer and metadata
    ///
    /// # Returns
    /// Processed audio data
    fn process_audio(&mut self, input: AudioData) -> Result<AudioData>;

    /// Get node type name
    fn node_type(&self) -> &str;
}
