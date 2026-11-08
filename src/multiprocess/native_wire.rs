//! MessagePack compatibility encoding directly into one exact native loan.
//! The sizing pass retains no wire bytes. It still reads/encodes the source, so
//! this is a copying codec, not the descriptor-based zero-copy worker protocol.
use super::{data_transfer, runner::NativePublisher};
use remotemedia_types::{CopyContext, CopyReason, RuntimeData};
use serde::Serialize;
use std::{
    io::{self, Write},
    mem::MaybeUninit,
};

pub(super) fn publish_runtime_data(
    publisher: &NativePublisher,
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<(), String> {
    let length = encoded_len(data, copies, data_transfer::MAX_SLICE_LEN)?;
    let mut sample = publisher
        .loan_slice_uninit(length)
        .map_err(|error| format!("loan: {error:?}"))?;
    encode_into(sample.payload_mut(), data, copies)?;
    // SAFETY: encode_into returns success only after every byte of this exact
    // loan was written. Any codec/bounds/length failure drops the unpublished loan.
    let sample = unsafe { sample.assume_init() };
    sample.send().map_err(|error| format!("send: {error:?}"))?;
    Ok(())
}

fn encoded_len(data: &RuntimeData, copies: &CopyContext, limit: usize) -> Result<usize, String> {
    // Authorize before even the sizing traversal reads a bulk payload. No output
    // materialization occurs during this pass, so it has no completed-copy event.
    copies
        .check(
            CopyReason::PortableSerialization,
            data.bulk_payload_bytes() as u64,
        )
        .map_err(|error| error.to_string())?;
    let mut counter = WireSize { bytes: 0, limit };
    data.serialize(&mut rmp_serde::Serializer::new(&mut counter).with_struct_map())
        .map_err(|error| format!("msgpack size: {error:?}"))?;
    Ok(counter.bytes)
}

fn encode_into(
    destination: &mut [MaybeUninit<u8>],
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<(), String> {
    let mut writer = LoanWriter {
        destination,
        written: 0,
    };
    data_transfer::write_with_copy_context(&mut writer, data, copies)?;
    writer.finish().map_err(|error| error.to_string())
}

struct WireSize {
    bytes: usize,
    limit: usize,
}
impl Write for WireSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native MessagePack size exceeds loan limit",
                )
            })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writes only initialized bytes; no &[u8] is ever formed over unwritten memory.
struct LoanWriter<'a> {
    destination: &'a mut [MaybeUninit<u8>],
    written: usize,
}
impl LoanWriter<'_> {
    fn finish(self) -> io::Result<()> {
        if self.written != self.destination.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native MessagePack length changed during encoding",
            ));
        }
        Ok(())
    }
}
impl Write for LoanWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .written
            .checked_add(bytes.len())
            .filter(|end| *end <= self.destination.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    "native MessagePack exceeded sized loan",
                )
            })?;
        for (out, byte) in self.destination[self.written..end].iter_mut().zip(bytes) {
            out.write(*byte);
        }
        self.written = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
