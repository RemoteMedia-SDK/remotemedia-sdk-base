//! Owning pipeline handles and explicit JavaScript buffer snapshots.
//!
//! Native handles retain immutable payload owners. Mutable JS Buffer access
//! explicitly materializes a snapshot and can be rejected under strict policy.
//!
//! # Wire Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Offset │ Field          │ Size     │ Description               │
//! ├────────┼────────────────┼──────────┼───────────────────────────┤
//! │ 0      │ data_type      │ 1 byte   │ Type discriminant (1-6)   │
//! │ 1      │ session_len    │ 2 bytes  │ Session ID length (LE)    │
//! │ 3      │ session_id     │ N bytes  │ UTF-8 session identifier  │
//! │ 3+N    │ timestamp_ns   │ 8 bytes  │ Unix timestamp (LE u64)   │
//! │ 11+N   │ payload_len    │ 4 bytes  │ Payload length (LE u32)   │
//! │ 15+N   │ payload        │ M bytes  │ Type-specific payload     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Audio Payload Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ sample_rate (4 bytes) | channels (2 bytes) | padding (2 bytes) │
//! │ num_samples (8 bytes) | samples (num_samples * 4 bytes f32 LE) │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Video Payload Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ width (4 bytes) | height (4 bytes) | format (1 byte)           │
//! │ codec (1 byte) | frame_number (8 bytes) | is_keyframe (1 byte) │
//! │ pixel_data (variable)                                          │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

#[cfg(test)]
use napi::bindgen_prelude::Buffer;
use napi::{Env, JsBuffer};
use napi_derive::napi;
use remotemedia_core::data::video::{PixelFormat, VideoCodec};
use remotemedia_core::data::RuntimeData;
use std::collections::HashMap;

use super::buffers::{self, InputBuffer};
use remotemedia_types::SharedBuffer;

/// Data type discriminants matching Rust RuntimeData enum
#[napi]
pub enum RuntimeDataType {
    Audio = 1,
    Video = 2,
    Text = 3,
    Tensor = 4,
    ControlMessage = 5,
    Numpy = 6,
    Json = 7,
    Binary = 8,
}

/// Audio buffer with owning native storage
///
/// Runtime storage is immutable. JavaScript receives an independent snapshot. Use Float32Array to access samples:
/// ```js
/// const buf = audioBuffer.getBuffer();
/// const samples = new Float32Array(buf.buffer, buf.byteOffset, audioBuffer.numSamples);
/// ```
#[napi]
pub struct AudioBuffer {
    /// Audio samples as f32 (4 bytes per sample, little-endian)
    samples: SharedBuffer<f32>,
    /// Sample rate in Hz
    sample_rate: u32,
    /// Number of channels
    channels: u32,
}

#[napi]
impl AudioBuffer {
    /// Create a new audio buffer from raw f32 samples (as bytes, little-endian)
    #[napi(constructor)]
    pub fn new(
        samples_buffer: InputBuffer,
        sample_rate: u32,
        channels: u32,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<Self> {
        let samples =
            buffers::import_audio(samples_buffer.into_buffer()?.as_ref(), require_zero_copy)?;

        Ok(Self {
            samples,
            sample_rate,
            channels,
        })
    }

    /// Get the raw sample buffer as an independent mutable JavaScript snapshot.
    ///
    /// Returns a counted copy; strict mode rejects nonempty buffer exports.
    /// Use Float32Array to interpret the samples:
    /// ```js
    /// const buf = audioBuffer.getBuffer();
    /// const samples = new Float32Array(buf.buffer, buf.byteOffset, audioBuffer.numSamples);
    /// ```
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_buffer(&self, env: Env, require_zero_copy: Option<bool>) -> napi::Result<JsBuffer> {
        buffers::export_audio(env, &self.samples, require_zero_copy)
    }

    /// Get sample rate
    #[napi(getter)]
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Get number of channels
    #[napi(getter)]
    pub fn channels(&self) -> u32 {
        self.channels
    }

    /// Get number of samples
    #[napi(getter)]
    pub fn num_samples(&self) -> u32 {
        self.samples.len() as u32
    }
}

/// Video frame with owning native storage
#[napi]
pub struct VideoFrame {
    /// Pixel data
    pixel_data: remotemedia_types::SharedBytes,
    /// Frame width
    width: u32,
    /// Frame height
    height: u32,
    /// Pixel format
    format: u8,
    /// Codec (0 = raw, 1 = VP8, 2 = H264, 3 = AV1)
    codec: u8,
    /// Frame number
    frame_number: u64,
    /// Is keyframe
    is_keyframe: bool,
}

#[napi]
impl VideoFrame {
    /// Create a new video frame
    #[napi(constructor)]
    pub fn new(
        pixel_data: InputBuffer,
        width: u32,
        height: u32,
        format: u8,
        codec: u8,
        frame_number: i64,
        is_keyframe: bool,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<Self> {
        Ok(Self {
            pixel_data: buffers::import_bytes(
                pixel_data.into_buffer()?.as_ref(),
                require_zero_copy,
            )?,
            width,
            height,
            format,
            codec,
            frame_number: frame_number as u64,
            is_keyframe,
        })
    }

    /// Get pixel data buffer as an independent mutable JavaScript snapshot.
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_buffer(&self, env: Env, require_zero_copy: Option<bool>) -> napi::Result<JsBuffer> {
        buffers::export_bytes(env, &self.pixel_data, require_zero_copy)
    }

    #[napi(getter)]
    pub fn width(&self) -> u32 {
        self.width
    }

    #[napi(getter)]
    pub fn height(&self) -> u32 {
        self.height
    }

    #[napi(getter)]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[napi(getter)]
    pub fn codec(&self) -> u8 {
        self.codec
    }

    #[napi(getter)]
    pub fn frame_number(&self) -> i64 {
        self.frame_number as i64
    }

    #[napi(getter)]
    pub fn is_keyframe(&self) -> bool {
        self.is_keyframe
    }
}

/// RuntimeData wrapper for Node.js with owning native storage
#[napi]
#[derive(Clone)]
pub struct NapiRuntimeData {
    pub(crate) inner: RuntimeData,
    copies: Option<remotemedia_types::CopyContext>,
}

impl NapiRuntimeData {
    pub(crate) fn from_runtime(inner: RuntimeData) -> Self {
        Self {
            copies: None,
            inner: inner.into_shared(),
        }
    }

    pub(crate) fn from_runtime_with_context(
        inner: RuntimeData,
        copies: &remotemedia_types::CopyContext,
    ) -> Self {
        let mut output = Self::from_runtime(inner);
        output.copies = Some(copies.clone());
        output
    }

    /// Get a reference to the inner RuntimeData (for Rust-side access)
    pub fn get_inner(&self) -> &RuntimeData {
        &self.inner
    }
}

#[napi]
impl NapiRuntimeData {
    /// Compare backing storage without exposing mutable aliases or addresses.
    #[napi]
    pub fn shares_buffer_with(&self, other: &NapiRuntimeData) -> napi::Result<bool> {
        use remotemedia_types::{PayloadStorage, TensorEncoding};
        let parts = |data: &RuntimeData| {
            data.clone()
                .into_shared_parts(TensorEncoding::Runtime)
                .map(|(_, payload)| payload)
                .map_err(|e| napi::Error::from_reason(e.to_string()))
        };
        Ok(match (parts(&self.inner)?, parts(&other.inner)?) {
            (PayloadStorage::Bytes(a), PayloadStorage::Bytes(b)) => a.shares_storage(&b),
            (PayloadStorage::Audio(a), PayloadStorage::Audio(b)) => a.shares_storage(&b),
            _ => false,
        })
    }

    /// Retain a binary subrange without materializing it.
    #[napi]
    pub fn slice_binary(&self, start: u32, end: u32) -> napi::Result<Self> {
        let RuntimeData::Binary(data) = &self.inner else {
            return Err(napi::Error::from_reason("Not binary data"));
        };
        let slice = data
            .slice(start as usize..end as usize)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let mut output = Self::from_runtime(RuntimeData::Binary(slice));
        output.copies = self.copies.clone();
        Ok(output)
    }

    /// Inspect a byte without exporting a writable Buffer.
    #[napi]
    pub fn read_byte(&self, index: u32) -> napi::Result<u32> {
        let data = match &self.inner {
            RuntimeData::Binary(data)
            | RuntimeData::Tensor { data, .. }
            | RuntimeData::Image { data, .. }
            | RuntimeData::Numpy { data, .. } => data,
            RuntimeData::Video { pixel_data, .. } => pixel_data,
            _ => return Err(napi::Error::from_reason("Not byte-backed data")),
        };
        data.get(index as usize)
            .copied()
            .map(u32::from)
            .ok_or_else(|| napi::Error::from_reason("byte index out of range"))
    }

    #[napi]
    pub fn read_audio_sample(&self, index: u32) -> napi::Result<f64> {
        let RuntimeData::Audio { samples, .. } = &self.inner else {
            return Err(napi::Error::from_reason("Not audio data"));
        };
        samples
            .get(index as usize)
            .copied()
            .map(f64::from)
            .ok_or_else(|| napi::Error::from_reason("sample index out of range"))
    }

    /// Create audio data
    #[napi(factory)]
    pub fn audio(
        samples_buffer: InputBuffer,
        sample_rate: u32,
        channels: u32,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<Self> {
        let samples =
            buffers::import_audio(samples_buffer.into_buffer()?.as_ref(), require_zero_copy)?;

        Ok(Self {
            copies: None,
            inner: RuntimeData::Audio {
                samples: samples.into(),
                sample_rate,
                channels,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
        })
    }

    /// Create video data
    #[napi(factory)]
    pub fn video(
        pixel_data: InputBuffer,
        width: u32,
        height: u32,
        format: u8,
        codec: Option<u8>,
        frame_number: i64,
        is_keyframe: bool,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<Self> {
        let pixel_format = match format {
            0 => PixelFormat::Unspecified,
            1 => PixelFormat::Yuv420p,
            2 => PixelFormat::I420,
            3 => PixelFormat::NV12,
            4 => PixelFormat::Rgb24,
            5 => PixelFormat::Rgba32,
            255 => PixelFormat::Encoded,
            _ => PixelFormat::Unspecified,
        };

        let video_codec = codec.and_then(|c| match c {
            1 => Some(VideoCodec::Vp8),
            2 => Some(VideoCodec::H264),
            3 => Some(VideoCodec::Av1),
            _ => None,
        });

        Ok(Self {
            copies: None,
            inner: RuntimeData::Video {
                pixel_data: buffers::import_bytes(
                    pixel_data.into_buffer()?.as_ref(),
                    require_zero_copy,
                )?,
                width,
                height,
                format: pixel_format,
                codec: video_codec,
                frame_number: frame_number as u64,
                timestamp_us: 0,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            },
        })
    }

    /// Create text data
    #[napi(factory)]
    pub fn text(text: String) -> Self {
        Self {
            copies: None,
            inner: RuntimeData::Text(text),
        }
    }

    /// Create binary data
    #[napi(factory)]
    pub fn binary(data: InputBuffer, require_zero_copy: Option<bool>) -> napi::Result<Self> {
        Ok(Self {
            copies: None,
            inner: RuntimeData::Binary(buffers::import_bytes(
                data.into_buffer()?.as_ref(),
                require_zero_copy,
            )?),
        })
    }

    /// Create tensor data
    #[napi(factory)]
    pub fn tensor(
        data: InputBuffer,
        shape: Vec<i32>,
        dtype: i32,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<Self> {
        Ok(Self {
            copies: None,
            inner: RuntimeData::Tensor {
                data: buffers::import_bytes(data.into_buffer()?.as_ref(), require_zero_copy)?,
                shape,
                dtype,
                metadata: None,
            },
        })
    }

    /// Create JSON data from a JavaScript object serialized to string
    #[napi(factory)]
    pub fn json(json_string: String) -> napi::Result<Self> {
        let value: serde_json::Value = serde_json::from_str(&json_string)
            .map_err(|e| napi::Error::from_reason(format!("Invalid JSON: {}", e)))?;
        Ok(Self {
            copies: None,
            inner: RuntimeData::Json(value),
        })
    }

    /// Create file reference data (Spec 001: RuntimeData.File)
    ///
    /// Creates a file reference that can be passed through the pipeline without
    /// loading file contents into memory.
    ///
    /// # Arguments
    /// * `path` - File path (absolute or relative)
    /// * `filename` - Optional original filename
    /// * `mime_type` - Optional MIME type hint
    /// * `size` - Optional file size in bytes
    /// * `offset` - Optional byte offset for range requests
    /// * `length` - Optional length for range requests
    /// * `stream_id` - Optional stream identifier for multi-track routing
    #[napi(factory)]
    pub fn file(
        path: String,
        filename: Option<String>,
        mime_type: Option<String>,
        size: Option<i64>,
        offset: Option<i64>,
        length: Option<i64>,
        stream_id: Option<String>,
    ) -> Self {
        Self {
            copies: None,
            inner: RuntimeData::File {
                path,
                filename,
                mime_type,
                size: size.map(|s| s as u64),
                offset: offset.map(|o| o as u64),
                length: length.map(|l| l as u64),
                stream_id,
            },
        }
    }

    /// Get the data type
    #[napi(getter)]
    pub fn data_type(&self) -> u8 {
        match &self.inner {
            RuntimeData::Audio { .. } => 1,
            RuntimeData::Video { .. } => 2,
            RuntimeData::Text(_) => 3,
            RuntimeData::Tensor { .. } => 4,
            RuntimeData::ControlMessage { .. } => 5,
            RuntimeData::Numpy { .. } => 6,
            RuntimeData::Json(_) => 7,
            RuntimeData::Binary(_) => 8,
            RuntimeData::File { .. } => 9,
            RuntimeData::Image { .. } => 10,
            // Device-resident; sessions transfer these to host before output.
            RuntimeData::DeviceTensor { .. } => 11,
        }
    }

    /// Get audio samples as an independent mutable JavaScript snapshot.
    ///
    /// Returns a counted copy; strict mode rejects nonempty buffer exports.
    /// Use Float32Array to interpret the samples:
    /// ```js
    /// const buf = data.getAudioSamples();
    /// const samples = new Float32Array(buf.buffer, buf.byteOffset, data.getAudioNumSamples());
    /// ```
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_audio_samples(
        &self,
        env: Env,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Audio { samples, .. } => buffers::export_audio_with_context(
                env,
                samples,
                require_zero_copy,
                self.copies.as_ref(),
            ),
            _ => Err(napi::Error::from_reason("Not audio data")),
        }
    }

    /// Get the number of audio samples
    #[napi]
    pub fn get_audio_num_samples(&self) -> napi::Result<u32> {
        match &self.inner {
            RuntimeData::Audio { samples, .. } => Ok(samples.len() as u32),
            _ => Err(napi::Error::from_reason("Not audio data")),
        }
    }

    /// Get audio sample rate
    #[napi]
    pub fn get_audio_sample_rate(&self) -> napi::Result<u32> {
        match &self.inner {
            RuntimeData::Audio { sample_rate, .. } => Ok(*sample_rate),
            _ => Err(napi::Error::from_reason("Not audio data")),
        }
    }

    /// Get audio channels
    #[napi]
    pub fn get_audio_channels(&self) -> napi::Result<u32> {
        match &self.inner {
            RuntimeData::Audio { channels, .. } => Ok(*channels),
            _ => Err(napi::Error::from_reason("Not audio data")),
        }
    }

    /// Get video pixel data as an independent mutable JavaScript snapshot.
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_video_pixels(
        &self,
        env: Env,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Video { pixel_data, .. } => buffers::export_bytes_with_context(
                env,
                pixel_data,
                require_zero_copy,
                self.copies.as_ref(),
            ),
            _ => Err(napi::Error::from_reason("Not video data")),
        }
    }

    /// Get video width
    #[napi]
    pub fn get_video_width(&self) -> napi::Result<u32> {
        match &self.inner {
            RuntimeData::Video { width, .. } => Ok(*width),
            _ => Err(napi::Error::from_reason("Not video data")),
        }
    }

    /// Get video height
    #[napi]
    pub fn get_video_height(&self) -> napi::Result<u32> {
        match &self.inner {
            RuntimeData::Video { height, .. } => Ok(*height),
            _ => Err(napi::Error::from_reason("Not video data")),
        }
    }

    /// Get text content
    #[napi]
    pub fn get_text(&self) -> napi::Result<String> {
        match &self.inner {
            RuntimeData::Text(s) => Ok(s.clone()),
            _ => Err(napi::Error::from_reason("Not text data")),
        }
    }

    /// Get binary data as an independent mutable JavaScript snapshot.
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_binary(&self, env: Env, require_zero_copy: Option<bool>) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Binary(b) => {
                buffers::export_bytes_with_context(env, b, require_zero_copy, self.copies.as_ref())
            }
            _ => Err(napi::Error::from_reason("Not binary data")),
        }
    }

    /// Get tensor data as an independent mutable JavaScript snapshot.
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_tensor_data(
        &self,
        env: Env,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Tensor { data, .. } => buffers::export_bytes_with_context(
                env,
                data,
                require_zero_copy,
                self.copies.as_ref(),
            ),
            _ => Err(napi::Error::from_reason("Not tensor data")),
        }
    }

    /// Get tensor shape
    #[napi]
    pub fn get_tensor_shape(&self) -> napi::Result<Vec<i32>> {
        match &self.inner {
            RuntimeData::Tensor { shape, .. } => Ok(shape.clone()),
            _ => Err(napi::Error::from_reason("Not tensor data")),
        }
    }

    /// Get JSON data as string
    #[napi]
    pub fn get_json(&self) -> napi::Result<String> {
        match &self.inner {
            RuntimeData::Json(v) => serde_json::to_string(v)
                .map_err(|e| napi::Error::from_reason(format!("Failed to serialize JSON: {}", e))),
            _ => Err(napi::Error::from_reason("Not JSON data")),
        }
    }

    /// Get text as an independent mutable UTF-8 snapshot.
    ///
    /// Use TextDecoder to convert to string if needed:
    /// ```js
    /// const buf = data.getTextBuffer();
    /// const text = new TextDecoder().decode(buf);
    /// ```
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_text_buffer(
        &self,
        env: Env,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Text(s) => buffers::export_bytes_with_context(
                env,
                s.as_bytes(),
                require_zero_copy,
                self.copies.as_ref(),
            ),
            _ => Err(napi::Error::from_reason("Not text data")),
        }
    }

    /// Get numpy array data as an independent mutable JavaScript snapshot.
    ///
    /// The returned buffer owns its storage and may outlive this handle.
    #[napi]
    pub fn get_numpy_data(
        &self,
        env: Env,
        require_zero_copy: Option<bool>,
    ) -> napi::Result<JsBuffer> {
        match &self.inner {
            RuntimeData::Numpy { data, .. } => buffers::export_bytes_with_context(
                env,
                data,
                require_zero_copy,
                self.copies.as_ref(),
            ),
            _ => Err(napi::Error::from_reason("Not numpy data")),
        }
    }

    /// Get numpy array shape
    #[napi]
    pub fn get_numpy_shape(&self) -> napi::Result<Vec<u32>> {
        match &self.inner {
            RuntimeData::Numpy { shape, .. } => Ok(shape.iter().map(|&s| s as u32).collect()),
            _ => Err(napi::Error::from_reason("Not numpy data")),
        }
    }

    /// Get numpy array dtype
    #[napi]
    pub fn get_numpy_dtype(&self) -> napi::Result<String> {
        match &self.inner {
            RuntimeData::Numpy { dtype, .. } => Ok(dtype.clone()),
            _ => Err(napi::Error::from_reason("Not numpy data")),
        }
    }

    /// Get file path
    #[napi]
    pub fn get_file_path(&self) -> napi::Result<String> {
        match &self.inner {
            RuntimeData::File { path, .. } => Ok(path.clone()),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file filename (optional)
    #[napi]
    pub fn get_file_filename(&self) -> napi::Result<Option<String>> {
        match &self.inner {
            RuntimeData::File { filename, .. } => Ok(filename.clone()),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file MIME type (optional)
    #[napi]
    pub fn get_file_mime_type(&self) -> napi::Result<Option<String>> {
        match &self.inner {
            RuntimeData::File { mime_type, .. } => Ok(mime_type.clone()),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file size in bytes (optional)
    #[napi]
    pub fn get_file_size(&self) -> napi::Result<Option<i64>> {
        match &self.inner {
            RuntimeData::File { size, .. } => Ok(size.map(|s| s as i64)),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file offset for byte range (optional)
    #[napi]
    pub fn get_file_offset(&self) -> napi::Result<Option<i64>> {
        match &self.inner {
            RuntimeData::File { offset, .. } => Ok(offset.map(|o| o as i64)),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file length for byte range (optional)
    #[napi]
    pub fn get_file_length(&self) -> napi::Result<Option<i64>> {
        match &self.inner {
            RuntimeData::File { length, .. } => Ok(length.map(|l| l as i64)),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }

    /// Get file stream_id for multi-track routing (optional)
    #[napi]
    pub fn get_file_stream_id(&self) -> napi::Result<Option<String>> {
        match &self.inner {
            RuntimeData::File { stream_id, .. } => Ok(stream_id.clone()),
            _ => Err(napi::Error::from_reason("Not file data")),
        }
    }
}

/// Pipeline execution result with owning native output handles. Outputs keep
/// the session's copy authority: snapshots of them follow its policy and are
/// charged to its ledger.
#[napi]
pub struct PipelineOutput {
    outputs: HashMap<String, RuntimeData>,
    copies: remotemedia_types::CopyContext,
    nodes: remotemedia_core::transport::NodeMaterializationCopies,
}

#[napi]
impl PipelineOutput {
    /// Get output node IDs
    #[napi]
    pub fn get_node_ids(&self) -> Vec<String> {
        self.outputs.keys().cloned().collect()
    }

    /// Get output for a specific node as NapiRuntimeData
    #[napi]
    pub fn get(&self, node_id: String) -> Option<NapiRuntimeData> {
        self.outputs
            .get(&node_id)
            .map(|data| NapiRuntimeData::from_runtime_with_context(data.clone(), &self.copies))
    }

    /// Copies charged to the execution's transport ledger, including snapshots
    /// of its outputs (JSON: boundary, policy and per-reason counts).
    #[napi]
    pub fn copy_report(&self) -> napi::Result<String> {
        serde_json::to_string(&self.copies.report())
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Copies charged to each node's materialization ledger, by node id.
    #[napi]
    pub fn node_copy_reports(&self) -> napi::Result<String> {
        serde_json::to_string(&self.nodes.reports())
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Check if output exists for node
    #[napi]
    pub fn has(&self, node_id: String) -> bool {
        self.outputs.contains_key(&node_id)
    }

    /// Get number of outputs
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.outputs.len() as u32
    }
}

/// Execute a pipeline with zero-copy data transfer
///
/// # Arguments
///
/// * `manifest_json` - Pipeline manifest as JSON string
/// * `inputs` - Map of node_id -> NapiRuntimeData
///
/// # Returns
///
/// PipelineOutput with owning native storage to results
#[napi(ts_return_type = "Promise<PipelineOutput>")]
pub fn execute_pipeline(
    env: Env,
    manifest_json: String,
    inputs: HashMap<String, &NapiRuntimeData>,
    pipeline_buffers: Option<super::buffer_config::PipelineBufferConfig>,
) -> napi::Result<napi::JsObject> {
    let inputs = owned_inputs(inputs);
    env.spawn_future(execute_pipeline_internal(
        manifest_json,
        inputs,
        None,
        pipeline_buffers,
    ))
}

/// Execute a pipeline with session ID for multiprocess Python nodes
///
/// # Arguments
///
/// * `manifest_json` - Pipeline manifest as JSON string
/// * `inputs` - Map of node_id -> NapiRuntimeData
/// * `session_id` - Unique session identifier for IPC channel namespacing
///
/// # Returns
///
/// PipelineOutput with owning native storage to results
#[napi(ts_return_type = "Promise<PipelineOutput>")]
pub fn execute_pipeline_with_session(
    env: Env,
    manifest_json: String,
    inputs: HashMap<String, &NapiRuntimeData>,
    session_id: String,
    pipeline_buffers: Option<super::buffer_config::PipelineBufferConfig>,
) -> napi::Result<napi::JsObject> {
    let inputs = owned_inputs(inputs);
    env.spawn_future(execute_pipeline_internal(
        manifest_json,
        inputs,
        Some(session_id),
        pipeline_buffers,
    ))
}

/// Take shared owners of the inputs while still on the JS thread. Borrowed
/// handles must not be read inside the future: it runs on a runtime thread
/// after the call returns, when JS may already have collected them.
fn owned_inputs(inputs: HashMap<String, &NapiRuntimeData>) -> HashMap<String, RuntimeData> {
    inputs
        .into_iter()
        .map(|(node, data)| (node, data.inner.clone()))
        .collect()
}

/// Run one unary execution on a real session (see `super::unary`). Inputs
/// share their native owners; nothing is copied to send them.
async fn execute_pipeline_internal(
    manifest_json: String,
    inputs: HashMap<String, RuntimeData>,
    session_id: Option<String>,
    pipeline_buffers: Option<super::buffer_config::PipelineBufferConfig>,
) -> napi::Result<PipelineOutput> {
    let result = super::unary::execute(
        &manifest_json,
        inputs,
        session_id,
        pipeline_buffers.as_ref(),
    )
    .await?;
    Ok(PipelineOutput {
        outputs: result.outputs,
        copies: result.transport,
        nodes: result.nodes,
    })
}

/// Get the runtime version
#[napi]
pub fn get_runtime_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Check if the Rust runtime is available
#[napi]
pub fn is_runtime_available() -> bool {
    true
}

// ============================================================================
// Streaming Session API
// ============================================================================

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Streaming session for continuous input/output pipeline execution.
///
/// Provides bidirectional streaming:
/// - `sendInput()` streams data into the pipeline
/// - `recvOutput()` receives processed outputs
/// - `close()` gracefully terminates the session
///
/// Backed by the same `PipelineExecutor` session as `createStreamingSession`
/// (whole-graph routing, node initialization, copy policy and ledgers); this
/// is the older, smaller JavaScript surface over it.
#[napi]
pub struct NapiStreamSession {
    session_id: String,
    /// Clone of the session's bounded input (backpressure on `sendInput`).
    input: std::sync::Mutex<Option<remotemedia_core::transport::SessionInputSender>>,
    /// The session itself; taken on close to join it.
    session: Mutex<Option<remotemedia_core::transport::SessionHandle>>,
    /// The session's transport copy authority, retained by outputs.
    copies: remotemedia_types::CopyContext,
    is_active: Arc<AtomicBool>,
}

#[napi]
impl NapiStreamSession {
    /// Get the session ID
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Check if the session is still active
    #[napi(getter)]
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::SeqCst)
    }

    /// Send input to the pipeline's source nodes. Awaits when the pipeline is
    /// saturated (ingress backpressure). The input's native owner is shared.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn send_input(&self, env: Env, data: &NapiRuntimeData) -> napi::Result<napi::JsObject> {
        // Take the owner and the sender on the JS thread; see `owned_inputs`.
        let data = data.inner.clone();
        let input = self.input.lock().unwrap_or_else(|e| e.into_inner()).clone();
        env.spawn_future(async move {
            let input = input.ok_or_else(|| napi::Error::from_reason("Session is closed"))?;
            input
                .send(remotemedia_core::transport::TransportData::new(data))
                .await
                .map_err(|e| napi::Error::from_reason(format!("Failed to send input: {}", e)))
        })
    }

    /// Receive the next output, or null once the session has closed and no
    /// more outputs are pending.
    #[napi]
    pub async fn recv_output(&self) -> napi::Result<Option<NapiRuntimeData>> {
        let mut session = self.session.lock().await;
        let Some(session) = session.as_mut() else {
            return Ok(None);
        };
        match session
            .recv_output()
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?
        {
            Some(output) => Ok(Some(NapiRuntimeData::from_runtime_with_context(
                output.data,
                &self.copies,
            ))),
            None => {
                self.is_active.store(false, Ordering::SeqCst);
                Ok(None)
            }
        }
    }

    /// Copies charged to the session's transport ledger (JSON report).
    #[napi]
    pub fn copy_report(&self) -> napi::Result<String> {
        serde_json::to_string(&self.copies.report())
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    /// Close the session and join it, so node cleanup has finished when this
    /// resolves. After close(), `isActive` is false.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        self.input.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.is_active.store(false, Ordering::SeqCst);
        let session = self.session.lock().await.take();
        if let Some(mut session) = session {
            session
                .close()
                .await
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            session
                .wait()
                .await
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        }
        Ok(())
    }
}

/// Create a streaming session for continuous pipeline execution
///
/// # Arguments
///
/// * `manifest_json` - Pipeline manifest as JSON string
/// * `pipeline_buffers` - Optional copy policy and retention budget
///
/// # Returns
///
/// A `NapiStreamSession` that provides bidirectional streaming I/O.
///
/// # Example
///
/// ```javascript
/// const session = await createStreamSession(manifestJson);
/// await session.sendInput(audioData);
/// const output = await session.recvOutput();
/// await session.close();
/// ```
#[napi]
pub async fn create_stream_session(
    manifest_json: String,
    pipeline_buffers: Option<super::buffer_config::PipelineBufferConfig>,
) -> napi::Result<NapiStreamSession> {
    let manifest: remotemedia_core::manifest::Manifest = serde_json::from_str(&manifest_json)
        .map_err(|e| napi::Error::from_reason(format!("Failed to parse manifest: {}", e)))?;
    let executor =
        super::streaming_session::plugin_executor(pipeline_buffers.as_ref(), None).await?;
    let session = executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(|e| napi::Error::from_reason(format!("Failed to create session: {}", e)))?;
    Ok(NapiStreamSession {
        session_id: session.session_id.clone(),
        input: std::sync::Mutex::new(session.input_sender()),
        copies: session.transport_copy_context(),
        session: Mutex::new(Some(session)),
        is_active: Arc::new(AtomicBool::new(true)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audio_buffer_creation() {
        let samples: Vec<f32> = vec![0.0, 0.5, 1.0, -0.5];
        let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        let buffer = Buffer::from(bytes);

        let audio = AudioBuffer::new(buffer.into(), 48000, 1, None).unwrap();
        assert_eq!(audio.sample_rate(), 48000);
        assert_eq!(audio.channels(), 1);
        assert_eq!(audio.num_samples(), 4);
    }

    #[test]
    fn test_napi_runtime_data_audio() {
        let samples: Vec<f32> = vec![0.1, 0.2, 0.3];
        let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        let buffer = Buffer::from(bytes);

        let data = NapiRuntimeData::audio(buffer.into(), 16000, 1, None).unwrap();
        assert_eq!(data.data_type(), 1); // Audio
        assert_eq!(data.get_audio_sample_rate().unwrap(), 16000);
        assert_eq!(data.get_audio_channels().unwrap(), 1);
    }

    #[test]
    fn test_napi_runtime_data_text() {
        let data = NapiRuntimeData::text("Hello, World!".to_string());
        assert_eq!(data.data_type(), 3); // Text
        assert_eq!(data.get_text().unwrap(), "Hello, World!");
    }

    // File type tests (Spec 001: RuntimeData.File)

    #[test]
    fn test_napi_runtime_data_file_minimal() {
        let data = NapiRuntimeData::file(
            "/tmp/test.bin".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(data.data_type(), 9); // File
        assert_eq!(data.get_file_path().unwrap(), "/tmp/test.bin");
        assert!(data.get_file_filename().unwrap().is_none());
        assert!(data.get_file_mime_type().unwrap().is_none());
        assert!(data.get_file_size().unwrap().is_none());
        assert!(data.get_file_offset().unwrap().is_none());
        assert!(data.get_file_length().unwrap().is_none());
        assert!(data.get_file_stream_id().unwrap().is_none());
    }

    #[test]
    fn test_napi_runtime_data_file_all_fields() {
        let data = NapiRuntimeData::file(
            "/data/input/video.mp4".to_string(),
            Some("video.mp4".to_string()),
            Some("video/mp4".to_string()),
            Some(104_857_600),
            Some(1_048_576),
            Some(65_536),
            Some("video_track".to_string()),
        );
        assert_eq!(data.data_type(), 9); // File
        assert_eq!(data.get_file_path().unwrap(), "/data/input/video.mp4");
        assert_eq!(
            data.get_file_filename().unwrap(),
            Some("video.mp4".to_string())
        );
        assert_eq!(
            data.get_file_mime_type().unwrap(),
            Some("video/mp4".to_string())
        );
        assert_eq!(data.get_file_size().unwrap(), Some(104_857_600));
        assert_eq!(data.get_file_offset().unwrap(), Some(1_048_576));
        assert_eq!(data.get_file_length().unwrap(), Some(65_536));
        assert_eq!(
            data.get_file_stream_id().unwrap(),
            Some("video_track".to_string())
        );
    }

    #[test]
    fn test_napi_runtime_data_file_byte_range() {
        let data = NapiRuntimeData::file(
            "/data/large_file.bin".to_string(),
            None,
            None,
            Some(1_073_741_824),    // 1 GB
            Some(10 * 1024 * 1024), // 10 MB offset
            Some(64 * 1024),        // 64 KB chunk
            None,
        );
        assert_eq!(data.get_file_path().unwrap(), "/data/large_file.bin");
        assert_eq!(data.get_file_size().unwrap(), Some(1_073_741_824));
        assert_eq!(data.get_file_offset().unwrap(), Some(10 * 1024 * 1024));
        assert_eq!(data.get_file_length().unwrap(), Some(64 * 1024));
    }

    #[test]
    fn test_file_getters_error_on_non_file() {
        let data = NapiRuntimeData::text("Not a file".to_string());
        assert!(data.get_file_path().is_err());
        assert!(data.get_file_filename().is_err());
        assert!(data.get_file_mime_type().is_err());
        assert!(data.get_file_size().is_err());
        assert!(data.get_file_offset().is_err());
        assert!(data.get_file_length().is_err());
        assert!(data.get_file_stream_id().is_err());
    }
}
