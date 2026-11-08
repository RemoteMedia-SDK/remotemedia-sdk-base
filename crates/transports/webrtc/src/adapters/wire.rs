//! Materialize portable output once into its final protobuf wire allocation.
use super::*;
use bytes::Bytes;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
use prost::Message;
use remotemedia_types::AudioSamples;

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

/// Count visible bulk bytes written into the wire buffer. Metadata is excluded.
/// Strict policy rejects before payload access or destination allocation.
pub fn encode_runtime_data_with_context(
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<Bytes, CopyRejected> {
    let prepared = PreparedRuntimeData::new(data, copies)?;
    let mut wire = Vec::with_capacity(prepared.encoded_len());
    prepared.append_to(&mut wire)?;
    Ok(wire.into())
}

enum Payload<'a> {
    Audio {
        samples: &'a AudioSamples,
        header: AudioBuffer,
    },
    Other(DataBuffer),
}

/// Plan for embedding runtime data directly in a larger protobuf message.
/// Construction authorizes before payload access; each append counts its bulk
/// transfer. Callers must reserve the full destination before appending.
pub(crate) struct PreparedRuntimeData<'a> {
    payload: Payload<'a>,
    copies: &'a CopyContext,
    bytes: usize,
}

impl<'a> PreparedRuntimeData<'a> {
    pub(crate) fn new(
        data: &'a RuntimeData,
        copies: &'a CopyContext,
    ) -> Result<Self, CopyRejected> {
        let bytes = data.bulk_payload_bytes();
        copies.check(CopyReason::PortableSerialization, bytes as u64)?;
        let payload = match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                metadata,
                ..
            } => Payload::Audio {
                samples,
                header: audio_metadata(*sample_rate, *channels, samples.len(), metadata),
            },
            _ => Payload::Other(encode_data_buffer(data)),
        };
        Ok(Self {
            payload,
            copies,
            bytes,
        })
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match &self.payload {
            Payload::Audio { header, .. } => field_len(audio_len(self.bytes, header)),
            Payload::Other(buffer) => buffer.encoded_len(),
        }
    }

    pub(crate) fn append_to(&self, wire: &mut Vec<u8>) -> Result<(), CopyRejected> {
        // Do not hide reallocation copies of an already written payload.
        assert!(
            wire.capacity() - wire.len() >= self.encoded_len(),
            "reserve complete wire destination"
        );
        self.copies
            .materialize(CopyReason::PortableSerialization, self.bytes as u64, || {
                match &self.payload {
                    Payload::Audio { samples, header } => {
                        // DataBuffer.audio and AudioBuffer.samples both use tag 1.
                        encode_key(1, WireType::LengthDelimited, wire);
                        encode_varint(audio_len(self.bytes, header) as u64, wire);
                        if self.bytes != 0 {
                            encode_key(1, WireType::LengthDelimited, wire);
                            encode_varint(self.bytes as u64, wire);
                            for sample in samples.as_slice() {
                                wire.extend_from_slice(&sample.to_le_bytes());
                            }
                        }
                        header
                            .encode(wire)
                            .expect("sized audio metadata destination");
                    }
                    Payload::Other(buffer) => {
                        buffer.encode(wire).expect("sized DataBuffer destination")
                    }
                }
            })
    }
}

fn field_len(bytes: usize) -> usize {
    key_len(1) + encoded_len_varint(bytes as u64) + bytes
}

fn audio_len(bytes: usize, header: &AudioBuffer) -> usize {
    (if bytes == 0 { 0 } else { field_len(bytes) }) + header.encoded_len()
}
