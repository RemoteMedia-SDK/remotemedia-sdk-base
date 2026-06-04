//! File-sink nodes — write `RuntimeData::Audio` / `RuntimeData::Video`
//! directly to disk in well-known interchange formats.
//!
//! ## Why split into two nodes
//!
//! Each node owns one file with one writer. Splitting matches the
//! existing per-input-type-per-node convention + lets the manifest
//! place them anywhere on the graph (audio sink near the TTS,
//! video sink near the renderer, both as taps off the main path).
//!
//! ## Output formats
//!
//! - [`AudioFileWriterNode`] → **WAV** (RIFF/WAVE) with IEEE
//!   32-bit float samples. No re-encoding or re-quantization.
//! - [`VideoFileWriterNode`] → **Y4M** (YUV4MPEG2) with C420jpeg
//!   YUV. RGB24 input is converted to YUV420p per frame.
//!
//! Both formats are ffmpeg-compatible. To combine into a single
//! playable MP4:
//!
//! ```bash
//! ffmpeg -i video.y4m -i audio.wav \
//!     -c:v libx264 -pix_fmt yuv420p -c:a aac out.mp4
//! ```

mod audio_wav;
mod video_frame_diff;
mod video_y4m;

pub use audio_wav::{AudioFileWriterConfig, AudioFileWriterNode};
pub use video_frame_diff::{VideoFrameDiffConfig, VideoFrameDiffNode};
pub use video_y4m::{VideoFileWriterConfig, VideoFileWriterNode};
