//! Immutable native receive owners and exclusive native producer loans.
use super::{
    buffers::output_buffer, pipeline::NapiRuntimeData, runtime_data,
    subscriber::native::NativeReceived,
};
use napi::{bindgen_prelude::Buffer, Env, Error, Result};
use napi_derive::napi;
use remotemedia_types::{CopyContext, CopyReason, RuntimeData, SharedBytes};

/// Retains a native sample. Slices and toBinary outputs keep it alive independently.
#[napi]
pub struct ReceivedSample {
    bytes: Option<SharedBytes>,
    copies: CopyContext,
    timestamp_ns: i64,
    size: u32,
}
impl ReceivedSample {
    pub(super) fn new(received: NativeReceived) -> Self {
        let size = received.bytes.len() as u32;
        Self {
            bytes: Some(received.bytes),
            copies: received.copies,
            timestamp_ns: received.timestamp_ns,
            size,
        }
    }
    fn bytes(&self) -> Result<&SharedBytes> {
        self.bytes
            .as_ref()
            .ok_or_else(|| Error::from_reason("Sample already released"))
    }
}
#[napi]
impl ReceivedSample {
    /// Counted snapshot into final JS-owned storage. It survives release/close.
    #[napi(getter)]
    pub fn buffer(&self, env: Env) -> Result<Buffer> {
        let bytes = self.bytes()?;
        let operation = self
            .copies
            .begin(CopyReason::BorrowedInput, bytes.len() as u64)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let mut output = output_buffer(env, bytes.len())?;
        output.as_mut().copy_from_slice(bytes.as_slice());
        operation.complete();
        Ok(output)
    }
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.size
    }
    #[napi(getter)]
    pub fn is_released(&self) -> bool {
        self.bytes.is_none()
    }
    /// Local arrival time in nanoseconds since the Unix epoch.
    #[napi(getter)]
    pub fn timestamp_ns(&self) -> i64 {
        self.timestamp_ns
    }
    #[napi]
    pub fn release(&mut self) -> Result<()> {
        self.bytes
            .take()
            .ok_or_else(|| Error::from_reason("Sample already released"))?;
        Ok(())
    }
    #[napi]
    pub fn read_byte(&self, index: u32) -> Result<u32> {
        self.bytes()?
            .as_slice()
            .get(index as usize)
            .map(|byte| *byte as u32)
            .ok_or_else(|| Error::from_reason("Sample index out of bounds"))
    }
    #[napi]
    pub fn slice(&self, start: u32, end: u32) -> Result<ReceivedSample> {
        let bytes = self
            .bytes()?
            .slice(start as usize..end as usize)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(Self::new(NativeReceived {
            bytes,
            copies: self.copies.clone(),
            timestamp_ns: self.timestamp_ns,
        }))
    }
    /// Owning handoff of raw bytes to the pipeline, without serialization/copying.
    #[napi]
    pub fn to_binary(&self) -> Result<NapiRuntimeData> {
        Ok(NapiRuntimeData::from_runtime(RuntimeData::Binary(
            self.bytes()?.clone(),
        )))
    }
    /// Parse legacy Python wire metadata directly; this does not decode MessagePack.
    #[napi]
    pub fn to_runtime_data(&self) -> Result<runtime_data::ParsedRuntimeData> {
        runtime_data::parse_runtime_data_header_bytes(self.bytes()?.as_slice())
    }
    /// Borrow audio metadata without exporting a mutable payload snapshot.
    #[napi]
    pub fn audio_metadata(&self, payload_offset: u32) -> Result<runtime_data::AudioMetadata> {
        runtime_data::parse_audio_metadata_bytes(self.bytes()?.as_slice(), payload_offset)
    }
    /// Borrow video metadata without exporting a mutable payload snapshot.
    #[napi]
    pub fn video_metadata(&self, payload_offset: u32) -> Result<runtime_data::VideoMetadata> {
        runtime_data::parse_video_metadata_bytes(self.bytes()?.as_slice(), payload_offset)
    }
    /// Parse file-reference metadata directly from the retained native payload.
    #[napi]
    pub fn file_metadata(&self, payload_offset: u32) -> Result<runtime_data::FileMetadata> {
        runtime_data::parse_file_metadata_bytes(self.bytes()?.as_slice(), payload_offset)
    }
    /// Explicit counted MessagePack compatibility decoding under the receive policy.
    #[napi]
    pub fn decode(&self) -> Result<NapiRuntimeData> {
        remotemedia_core::multiprocess::data_transfer::from_bytes_with_copy_context(
            self.bytes()?.as_slice(),
            &self.copies,
        )
        .map(NapiRuntimeData::from_runtime)
        .map_err(Error::from_reason)
    }
}

mod loan;
pub use loan::LoanedSample;
