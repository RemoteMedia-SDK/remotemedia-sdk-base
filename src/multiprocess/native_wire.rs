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
    publish_runtime_data_with_limit(publisher, data, copies, data_transfer::MAX_SLICE_LEN)
}

/// Encode compatibility MessagePack directly into an exact native loan.
/// This is an attributed serialization copy, not a descriptor publication.
pub fn publish_runtime_data_with_limit(
    publisher: &iceoryx2::port::publisher::Publisher<iceoryx2::service::ipc::Service, [u8], ()>,
    data: &RuntimeData,
    copies: &CopyContext,
    limit: usize,
) -> Result<(), String> {
    encode_runtime_data(publisher, data, copies, limit)?
        .send()
        .map_err(|error| format!("send: {error:?}"))?;
    Ok(())
}

/// An encoded, not yet published, native sample.
pub(super) type EncodedSample =
    iceoryx2::sample_mut::SampleMut<iceoryx2::service::ipc::Service, [u8], ()>;

/// Encode into an exact native loan without publishing it.
pub(super) fn encode_runtime_data(
    publisher: &iceoryx2::port::publisher::Publisher<iceoryx2::service::ipc::Service, [u8], ()>,
    data: &RuntimeData,
    copies: &CopyContext,
    limit: usize,
) -> Result<EncodedSample, String> {
    encode_framed(publisher, data, copies, limit, 0, |_| Ok(()))
}

/// Encode into an exact native loan that starts with a `prefix`-byte frame
/// header, without publishing it. `fill` runs after the body is encoded (so it
/// can describe the encode itself) and must initialize every prefix byte.
/// `limit` bounds the encoded body; the loan is `prefix` bytes longer.
pub(super) fn encode_framed(
    publisher: &iceoryx2::port::publisher::Publisher<iceoryx2::service::ipc::Service, [u8], ()>,
    data: &RuntimeData,
    copies: &CopyContext,
    limit: usize,
    prefix: usize,
    fill: impl FnOnce(&mut [MaybeUninit<u8>]) -> Result<(), String>,
) -> Result<EncodedSample, String> {
    let body = encoded_len(data, copies, limit)?;
    let length = prefix
        .checked_add(body)
        .ok_or("native frame length overflow")?;
    let mut sample = publisher
        .loan_slice_uninit(length)
        .map_err(|error| format!("loan: {error:?}"))?;
    let (head, tail) = sample.payload_mut().split_at_mut(prefix);
    encode_into(tail, data, copies)?;
    fill(head)?;
    // SAFETY: encode_into returns success only after every body byte of this
    // exact loan was written, and `fill` initializes every prefix byte. Any
    // failure drops the unpublished loan.
    Ok(unsafe { sample.assume_init() })
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
