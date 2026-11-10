//! Retain incoming wire chunks; only fragmented events need contiguous staging.
use crate::codec;
use bytes::Bytes;
use remotemedia_core::{transport::TransportData, Result};
use remotemedia_types::{CopyContext, CopyReason};
use std::collections::VecDeque;

const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct Frames {
    chunks: VecDeque<Bytes>,
    chunk: usize,
    offset: usize,
    scanned: usize,
    line_has_data: bool,
}

impl Frames {
    pub fn push(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.chunks.push_back(chunk);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn next(&mut self, copies: &CopyContext) -> Result<Option<Bytes>> {
        while let Some(chunk) = self.chunks.get(self.chunk) {
            while self.offset < chunk.len() {
                let byte = chunk[self.offset];
                self.offset += 1;
                self.scanned += 1;
                if self.scanned > MAX_EVENT_BYTES {
                    return Err(codec::error("SSE event exceeds 16 MiB"));
                }
                if byte == b'\n' && !self.line_has_data {
                    return self.take_frame(copies).map(Some);
                }
                match byte {
                    b'\n' => self.line_has_data = false,
                    b'\r' => {}
                    _ => self.line_has_data = true,
                }
            }
            self.chunk += 1;
            self.offset = 0;
        }
        Ok(None)
    }

    fn take_frame(&mut self, copies: &CopyContext) -> Result<Bytes> {
        let size = self.scanned;
        let output = if self.chunk == 0 {
            self.chunks.front_mut().unwrap().split_to(size)
        } else {
            copies
                .materialize(CopyReason::VecMaterialization, size as u64, || {
                    let mut output = Vec::with_capacity(size);
                    let mut remaining = size;
                    while remaining > 0 {
                        let first = self.chunks.front_mut().unwrap();
                        let take = remaining.min(first.len());
                        output.extend_from_slice(&first.split_to(take));
                        remaining -= take;
                        if first.is_empty() {
                            self.chunks.pop_front();
                        }
                    }
                    Bytes::from(output)
                })
                .map_err(codec::error)?
        };
        if self.chunks.front().is_some_and(Bytes::is_empty) {
            self.chunks.pop_front();
        }
        self.chunk = 0;
        self.offset = 0;
        self.scanned = 0;
        self.line_has_data = false;
        Ok(output)
    }
}

pub(crate) fn decode(frame: &[u8], copies: &CopyContext) -> Result<Option<TransportData>> {
    let text = std::str::from_utf8(frame).map_err(codec::error)?;
    let mut event = "message";
    let mut data = Vec::new();
    for line in text.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => event = value,
            "data" => data.push(value),
            _ => {}
        }
    }
    if event == "error" {
        return Err(codec::error(format!(
            "SSE server error: {}",
            data.join("\n")
        )));
    }
    if event != "message" || data.is_empty() {
        return Ok(None);
    }
    if data.len() == 1 {
        return codec::decode(data[0].as_bytes(), copies).map(Some);
    }
    let length = data.iter().map(|line| line.len()).sum::<usize>() + data.len() - 1;
    let json = copies
        .materialize(CopyReason::VecMaterialization, length as u64, || {
            data.join("\n")
        })
        .map_err(codec::error)?;
    codec::decode(json.as_bytes(), copies).map(Some)
}
