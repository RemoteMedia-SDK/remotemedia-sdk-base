//! A request or response body kept as the network delivered it: chunks are
//! retained, not joined, and protobuf decoding reads across them. A bulk
//! field inside one chunk keeps that chunk as its storage; only a field that
//! spans chunks is assembled (framing, not counted as a payload copy).
use crate::codec;
use bytes::{Buf, Bytes};
use futures::{Stream, StreamExt};
use prost::Message;
use remotemedia_core::Result;

pub(crate) struct Body(Vec<Bytes>);

impl Body {
    pub async fn collect<E: std::fmt::Display>(
        stream: impl Stream<Item = std::result::Result<Bytes, E>>,
        limit: usize,
    ) -> Result<Self> {
        futures::pin_mut!(stream);
        let mut size = 0usize;
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(codec::error)?;
            size = size
                .checked_add(chunk.len())
                .filter(|size| *size <= limit)
                .ok_or_else(|| codec::error("HTTP body exceeds size limit"))?;
            if !chunk.is_empty() {
                chunks.push(chunk);
            }
        }
        Ok(Self(chunks))
    }

    /// Decode one protobuf message from the whole body.
    pub fn decode<M: Message + Default>(&self) -> Result<M> {
        let remaining = self.0.iter().map(Bytes::len).sum();
        M::decode(Chunks {
            chunks: &self.0,
            index: 0,
            offset: 0,
            remaining,
        })
        .map_err(|error| remotemedia_core::Error::InvalidData(format!("HTTP body: {error}")))
    }
}

struct Chunks<'a> {
    chunks: &'a [Bytes],
    index: usize,
    offset: usize,
    remaining: usize,
}

impl Buf for Chunks<'_> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.chunks
            .get(self.index)
            .map_or(&[], |chunk| &chunk[self.offset..])
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.remaining, "advance past end of body");
        self.remaining -= count;
        while count > 0 {
            let left = self.chunks[self.index].len() - self.offset;
            if count < left {
                self.offset += count;
                return;
            }
            count -= left;
            self.index += 1;
            self.offset = 0;
        }
        // Skip to the next byte, so `chunk` never returns an empty slice early.
        while self
            .chunks
            .get(self.index)
            .is_some_and(|chunk| self.offset == chunk.len())
        {
            self.index += 1;
            self.offset = 0;
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        if let Some(chunk) = self.chunks.get(self.index) {
            if chunk.len() - self.offset >= len {
                let bytes = chunk.slice(self.offset..self.offset + len);
                self.advance(len);
                return bytes;
            }
        }
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            let take = (len - bytes.len()).min(self.chunk().len());
            bytes.extend_from_slice(&self.chunk()[..take]);
            self.advance(take);
        }
        bytes.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ExecuteRequest;

    #[tokio::test]
    async fn decodes_across_chunks_and_keeps_in_chunk_fields_in_place() {
        let request = ExecuteRequest {
            manifest_json: "{\"version\":\"v1\"}".into(),
            input: None,
        };
        let wire = request.encode_to_vec();
        for size in [1, 3, wire.len()] {
            let chunks: Vec<_> = wire
                .chunks(size)
                .map(|chunk| Ok::<_, std::convert::Infallible>(Bytes::copy_from_slice(chunk)))
                .collect();
            let body = Body::collect(futures::stream::iter(chunks), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.decode::<ExecuteRequest>().unwrap(), request);
        }

        let payload = Bytes::from(vec![7u8; 64]);
        let mut chunks = Chunks {
            chunks: std::slice::from_ref(&payload),
            index: 0,
            offset: 0,
            remaining: payload.len(),
        };
        chunks.advance(8);
        let field = chunks.copy_to_bytes(32);
        assert_eq!(field.as_ptr(), payload[8..].as_ptr());
    }

    #[tokio::test]
    async fn oversized_body_is_refused() {
        let chunks = vec![Ok::<_, std::convert::Infallible>(Bytes::from(vec![0u8; 8]))];
        assert!(Body::collect(futures::stream::iter(chunks), 4)
            .await
            .is_err());
    }
}
