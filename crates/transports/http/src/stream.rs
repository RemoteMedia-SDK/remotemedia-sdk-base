//! Client side of the output stream: varint-delimited `StreamEvent`s.
//! Incoming chunks are retained; an event inside one chunk is a view of it,
//! and only an event split across chunks is assembled (counted as
//! `VecMaterialization`, so a strict context refuses it).
use crate::codec::{self, stream_event::Event, StreamEvent};
use bytes::Bytes;
use prost::Message;
use remotemedia_core::{transport::TransportData, Result};
use remotemedia_types::{CopyContext, CopyReason};
use std::collections::VecDeque;

const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct Events {
    chunks: VecDeque<Bytes>,
    available: usize,
}

impl Events {
    pub fn push(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.available += chunk.len();
            self.chunks.push_back(chunk);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.available == 0
    }

    /// The next complete event's bytes (without its length prefix), or
    /// `None` until more of it has arrived.
    pub fn next(&mut self, copies: &CopyContext) -> Result<Option<Bytes>> {
        let mut length = 0u64;
        let mut prefix = 0usize;
        let mut complete = false;
        for byte in self.chunks.iter().flat_map(|chunk| chunk.iter()).take(10) {
            length |= u64::from(byte & 0x7f) << (7 * prefix);
            prefix += 1;
            if byte & 0x80 == 0 {
                complete = true;
                break;
            }
        }
        if !complete {
            return if prefix == 10 {
                Err(codec::error("invalid stream event length"))
            } else {
                Ok(None)
            };
        }
        let length = usize::try_from(length)
            .ok()
            .filter(|length| *length <= MAX_EVENT_BYTES)
            .ok_or_else(|| codec::error("stream event exceeds 16 MiB"))?;
        if self.available < prefix + length {
            return Ok(None);
        }
        if length == 0 {
            self.skip(prefix);
            return Ok(Some(Bytes::new()));
        }
        // Check the copy before consuming anything, so a refused event
        // leaves the stream as it was.
        self.skip_empty();
        let in_one_chunk = self
            .chunks
            .front()
            .is_some_and(|front| front.len() >= prefix + length);
        if in_one_chunk {
            self.skip(prefix);
            return Ok(Some(self.split(length)));
        }
        copies
            .check(CopyReason::VecMaterialization, length as u64)
            .map_err(codec::error)?;
        self.skip(prefix);
        let event = copies
            .materialize(CopyReason::VecMaterialization, length as u64, || {
                let mut event = Vec::with_capacity(length);
                while event.len() < length {
                    let take = (length - event.len()).min(self.chunks[0].len());
                    event.extend_from_slice(&self.split(take));
                }
                Bytes::from(event)
            })
            .map_err(codec::error)?;
        Ok(Some(event))
    }

    fn skip_empty(&mut self) {
        while self.chunks.front().is_some_and(Bytes::is_empty) {
            self.chunks.pop_front();
        }
    }

    fn skip(&mut self, count: usize) {
        let mut left = count;
        while left > 0 {
            left -= self.split(left).len();
        }
    }

    /// Up to `count` bytes from the front chunk.
    fn split(&mut self, count: usize) -> Bytes {
        self.skip_empty();
        let front = self.chunks.front_mut().expect("bytes are available");
        let bytes = front.split_to(count.min(front.len()));
        self.available -= bytes.len();
        self.skip_empty();
        bytes
    }
}

/// Decode one event: `Some` frame, `None` for a keepalive, or the server's
/// error.
pub(crate) fn decode(event: Bytes, copies: &CopyContext) -> Result<Option<TransportData>> {
    match StreamEvent::decode(event).map_err(codec::error)?.event {
        None => Ok(None),
        Some(Event::Frame(buffer)) => codec::frame_from_buffer(buffer, copies).map(Some),
        Some(Event::Error(message)) => Err(codec::error(format!("server error: {message}"))),
    }
}
