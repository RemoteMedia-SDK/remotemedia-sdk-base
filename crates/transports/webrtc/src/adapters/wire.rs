//! Materialize portable output once into its final protobuf wire allocation.
use super::*;
use bytes::Bytes;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
use prost::Message;

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

/// Count visible bulk bytes written into the wire buffer. Metadata is excluded.
/// Strict policy rejects before payload access or destination allocation.
pub fn encode_runtime_data_with_context(
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<Bytes, CopyRejected> {
    copies.materialize(
        CopyReason::PortableSerialization,
        data.bulk_payload_bytes() as u64,
        || match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                metadata,
                ..
            } => {
                let header = audio_metadata(*sample_rate, *channels, samples.len(), metadata);
                encode_audio(samples.as_slice(), header).into()
            }
            _ => encode_data_buffer(data).encode_to_vec().into(),
        },
    )
}

fn encode_audio(samples: &[f32], header: AudioBuffer) -> Vec<u8> {
    // DataBuffer.audio and AudioBuffer.samples are both field 1. Keep these
    // two framing tags synchronized with the generated schema via wire tests.
    // The generated encoder still owns every other audio field and its defaults.
    let bytes = std::mem::size_of_val(samples);
    let sample_field = if bytes == 0 {
        0
    } else {
        key_len(1) + encoded_len_varint(bytes as u64) + bytes
    };
    let inner_len = sample_field + header.encoded_len();
    let wire_len = key_len(1) + encoded_len_varint(inner_len as u64) + inner_len;
    let mut wire = Vec::with_capacity(wire_len);
    encode_key(1, WireType::LengthDelimited, &mut wire);
    encode_varint(inner_len as u64, &mut wire);
    if bytes != 0 {
        encode_key(1, WireType::LengthDelimited, &mut wire);
        encode_varint(bytes as u64, &mut wire);
        for sample in samples {
            wire.extend_from_slice(&sample.to_le_bytes());
        }
    }
    header
        .encode(&mut wire)
        .expect("sized audio metadata destination");
    debug_assert_eq!(wire.len(), wire_len);
    wire
}
