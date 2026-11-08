//! Retain immutable FFmpeg packet storage; aggregate only when wire contiguity requires it.
use super::codec::{CodecError, Result};
use ac_ffmpeg::{
    codec::Encoder,
    packet::{Packet, PacketMut},
};
use remotemedia_types::{CopyContext, CopyReason, SharedBytes};

/// Private owner: no mutable packet handle escapes after publication. ac-ffmpeg's
/// immutable Packet retains its AVBuffer reference and makes mutations copy-on-write.
struct PacketOwner(Packet);

impl AsRef<[u8]> for PacketOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.data()
    }
}

/// Reuse a complete packet owner, including FFmpeg's required input padding.
/// Arbitrary buffers and partial ranges need a padded FFmpeg allocation.
pub(super) fn decoder_packet(data: SharedBytes, copies: &CopyContext) -> Result<Packet> {
    if let Some(owner) = data.owner::<PacketOwner>() {
        if data.storage_range() == (0..owner.0.data().len()) {
            return Ok(owner.0.clone());
        }
    }
    let attempt = copies
        .begin(CopyReason::ForeignPoolIngress, data.len() as u64)
        .map_err(|error| CodecError::DecodingFailed(error.to_string()))?;
    let mut packet = PacketMut::new(data.len());
    packet.data_mut().copy_from_slice(&data);
    attempt.complete();
    Ok(packet.freeze())
}

pub(super) fn take_encoded(
    encoder: &mut ac_ffmpeg::codec::video::VideoEncoder,
    copies: &CopyContext,
) -> Result<(SharedBytes, bool)> {
    let mut packets = Vec::new();
    while let Some(packet) = encoder.take().map_err(|error| {
        CodecError::EncodingFailed(format!("Failed to take packet from encoder: {error}"))
    })? {
        packets.push(packet);
    }
    publish_packets(packets, copies)
}

fn publish_packets(mut packets: Vec<Packet>, copies: &CopyContext) -> Result<(SharedBytes, bool)> {
    // Empty packets still contribute flags but do not force an otherwise avoidable copy.
    let keyframe = packets.iter().any(Packet::is_key);
    packets.retain(|packet| !packet.data().is_empty());
    if packets.len() <= 1 {
        return Ok((
            packets.pop().map_or_else(SharedBytes::default, |packet| {
                SharedBytes::from_owner(PacketOwner(packet))
            }),
            keyframe,
        ));
    }
    let bytes = packets.iter().try_fold(0usize, |bytes, packet| {
        bytes.checked_add(packet.data().len()).ok_or_else(|| {
            CodecError::EncodingFailed("Encoded packet size exceeds addressable storage".into())
        })
    })?;
    let attempt = copies
        .begin(CopyReason::VecMaterialization, bytes as u64)
        .map_err(|error| CodecError::EncodingFailed(error.to_string()))?;
    let mut output = Vec::new();
    output.try_reserve_exact(bytes).map_err(|error| {
        CodecError::EncodingFailed(format!("Cannot allocate aggregate encoded packet: {error}"))
    })?;
    for packet in packets {
        output.extend_from_slice(packet.data());
    }
    attempt.complete();
    Ok((output.into(), keyframe))
}

#[cfg(test)]
#[path = "packets_tests.rs"]
mod tests;
