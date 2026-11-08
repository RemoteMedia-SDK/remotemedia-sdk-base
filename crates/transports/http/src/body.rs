//! Retain network chunks and deserialize across them without a full-body snapshot.
use crate::codec;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use remotemedia_core::{manifest::Manifest, Result};
use remotemedia_types::CopyContext;
use serde::{de::DeserializeOwned, Deserialize};
use std::io::Read;

pub(crate) struct JsonBody(Vec<Bytes>);

impl JsonBody {
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

    pub fn decode<T: DeserializeOwned>(&self, copies: &CopyContext) -> Result<T> {
        if self.0.len() == 1 {
            return codec::decode(&self.0[0], copies);
        }
        let mut decoder = serde_json::Deserializer::from_reader(Reader {
            chunks: &self.0,
            index: 0,
            offset: 0,
        });
        let value = copies.deserialize(&mut decoder).map_err(codec::error)?;
        decoder.end().map_err(codec::error)?;
        Ok(value)
    }

    /// Only the manifest is decoded here. Serde skips the unknown input field;
    /// it never constructs RuntimeData, payload vectors or a raw JSON snapshot.
    pub fn manifest(&self) -> Result<Manifest> {
        #[derive(Deserialize)]
        struct Envelope {
            manifest: Manifest,
        }
        self.decode::<Envelope>(CopyContext::legacy())
            .map(|value| value.manifest)
    }
}

struct Reader<'a> {
    chunks: &'a [Bytes],
    index: usize,
    offset: usize,
}
impl Read for Reader<'_> {
    fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
        while let Some(chunk) = self.chunks.get(self.index) {
            if self.offset == chunk.len() {
                self.index += 1;
                self.offset = 0;
                continue;
            }
            let count = destination.len().min(chunk.len() - self.offset);
            destination[..count].copy_from_slice(&chunk[self.offset..self.offset + count]);
            self.offset += count;
            return Ok(count);
        }
        Ok(0)
    }
}
