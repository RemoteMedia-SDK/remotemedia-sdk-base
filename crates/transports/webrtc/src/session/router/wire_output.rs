//! Retain wire owners while fitting framed messages to the negotiated SCTP cap.
use crate::channels::{ChunkInfo, DataChannelMessage, DataChannelSplitter, MAX_CHUNK_SIZE};
use crate::{Error, Result};
use bytes::Bytes;
use webrtc::api::setting_engine::SctpMaxMessageSize;

pub(super) async fn message_size_limit(peer: &crate::peer::PeerConnection) -> Result<usize> {
    // PeerConnection uses the default SettingEngine send cap. The peer may
    // advertise a smaller receive cap; zero means unlimited, not zero bytes.
    let local = SctpMaxMessageSize::DEFAULT_MESSAGE_SIZE;
    let remote = peer
        .peer_connection()
        .remote_description()
        .await
        .ok_or_else(|| Error::DataChannelError("remote description is not available".into()))?;
    let parsed = remote
        .unmarshal()
        .map_err(|e| Error::SdpError(e.to_string()))?;
    let advertised = parsed
        .media_descriptions
        .iter()
        .find(|m| m.media_name.media == "application")
        .and_then(|m| m.attribute("max-message-size").flatten())
        .map(str::parse::<u32>)
        .transpose()
        .map_err(|e| Error::SdpError(format!("invalid max-message-size: {e}")))?
        .unwrap_or(local);
    Ok(if advertised == 0 {
        local
    } else {
        local.min(advertised)
    } as usize)
}

pub(super) fn prepare_messages(
    encoded: Bytes,
    stream_id: &str,
    limit: usize,
) -> Result<Vec<DataChannelMessage>> {
    let single = DataChannelMessage::runtime_data(encoded.clone());
    if single
        .encoded_len()
        .map_err(|e| Error::EncodingError(e.to_string()))?
        <= limit.min(MAX_CHUNK_SIZE)
    {
        return Ok(vec![single]);
    }
    let message_id = uuid::Uuid::new_v4().to_string();
    // Maximal index/count widths and final=true bound every chunk's metadata.
    let probe = DataChannelMessage::chunk(ChunkInfo::new(
        stream_id,
        &message_id,
        u32::MAX,
        Some(u32::MAX),
        true,
        Bytes::new(),
    ));
    let header = probe
        .encoded_len()
        .map_err(|e| Error::EncodingError(e.to_string()))?;
    // Adding bytes creates its tag/length and can grow the enclosing length.
    let growth = 1 + 2 * prost::encoding::encoded_len_varint(limit as u64);
    let payload_limit = limit
        .checked_sub(header + growth)
        .filter(|n| *n > 0)
        .ok_or_else(|| Error::DataChannelError("SCTP limit cannot fit chunk metadata".into()))?
        .min(MAX_CHUNK_SIZE);
    DataChannelSplitter::with_max_chunk_size(stream_id, message_id, payload_limit)
        .split_owned(encoded)
        .map(|chunks| chunks.into_iter().map(DataChannelMessage::chunk).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_output_chunks_fit_framing_limits_and_retain_input_ranges() {
        assert!(prepare_messages(Bytes::from(vec![7; 200_000]), "session", 128).is_err());
        let source = Bytes::from(vec![7; 100_000]);
        for limit in [128, 255, 256, 16_383, 16_384, 65_536] {
            let chunks = prepare_messages(source.clone(), "session", limit).unwrap();
            let mut offset = 0;
            for message in chunks {
                assert!(message.encoded_len().unwrap() <= limit);
                let DataChannelMessage::Chunk { chunk, .. } = message else {
                    panic!("chunk expected")
                };
                assert_eq!(chunk.data.as_ptr(), source.as_ptr().wrapping_add(offset));
                offset += chunk.data.len();
            }
            assert_eq!(offset, source.len());
        }
        assert!(prepare_messages(source.clone(), "session", 1).is_err());
        assert!(prepare_messages(source, &"x".repeat(1000), 128).is_err());
    }
}
