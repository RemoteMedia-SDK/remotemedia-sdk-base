//! In-band call frames for native copy reporting v2.
//!
//! Each call's accounting travels inside the iceoryx2 samples the call already
//! exchanges, so a call costs no control-channel round trip:
//!
//! - **Input** (host → worker): `RMI2 | call u64 | boundary_len u16 | boundary | MessagePack`.
//!   The worker records its framework copies for the call in a per-call ledger
//!   named after the host's boundary.
//! - **Output** (worker → host): `RMO2 | call u64 | status u8 | reasons u8 | 0u16 |
//!   per-reason counts | body`. The body is the MessagePack output (status 0) or a
//!   UTF-8 error (status 1). Counts are the call's per-reason totals, which the
//!   host replays into the caller's ledger before decoding the body, so accounting
//!   is applied atomically with the output it describes.
//!
//! All integers are little-endian.
use super::*;
use remotemedia_types::{CopyEvent, CopyLedger};
use std::mem::MaybeUninit;

const INPUT_MAGIC: [u8; 4] = *b"RMI2";
const OUTPUT_MAGIC: [u8; 4] = *b"RMO2";
const MAX_BOUNDARY: usize = 1024;
const INPUT_FIXED: usize = 4 + 8 + 2;
/// Per reason: completed, copied_bytes, failed, failed_requested_bytes,
/// rejected, rejected_bytes.
const COUNT_FIELDS: usize = 6;
const OUTPUT_FIXED: usize = 16;
/// Output prefix length; the body follows it.
pub(in crate::multiprocess) const OUTPUT_PREFIX: usize =
    OUTPUT_FIXED + COPY_REASONS.len() * COUNT_FIELDS * 8;
/// Upper bound on operations per reason per call, so a malformed frame cannot
/// make the host replay an unbounded number of ledger events.
const MAX_OPERATIONS: u64 = 1 << 20;
/// Error bodies are truncated to this many characters.
pub(in crate::multiprocess) const MAX_ERROR_CHARS: usize = 256;

/// Byte length of the input prefix for `boundary`.
pub(in crate::multiprocess) fn input_prefix_len(boundary: &str) -> Result<usize, String> {
    if boundary.len() > MAX_BOUNDARY {
        return Err(format!(
            "native copy boundary exceeds {MAX_BOUNDARY} bytes"
        ));
    }
    Ok(INPUT_FIXED + boundary.len())
}

/// Fill an input prefix of exactly `input_prefix_len(boundary)` bytes.
pub(in crate::multiprocess) fn write_input(
    destination: &mut [MaybeUninit<u8>],
    call: u64,
    boundary: &str,
) -> Result<(), String> {
    if destination.len() != input_prefix_len(boundary)? {
        return Err("native input prefix length mismatch".into());
    }
    let (fixed, name) = destination.split_at_mut(INPUT_FIXED);
    let mut head = [0u8; INPUT_FIXED];
    head[..4].copy_from_slice(&INPUT_MAGIC);
    head[4..12].copy_from_slice(&call.to_le_bytes());
    head[12..14].copy_from_slice(&(boundary.len() as u16).to_le_bytes());
    put(fixed, &head);
    put(name, boundary.as_bytes());
    Ok(())
}

/// A parsed input frame borrowing the sample payload.
pub(in crate::multiprocess) struct Input<'a> {
    pub(in crate::multiprocess) call: u64,
    pub(in crate::multiprocess) boundary: &'a str,
    pub(in crate::multiprocess) body: &'a [u8],
}

pub(in crate::multiprocess) fn parse_input(bytes: &[u8]) -> Result<Input<'_>, String> {
    let fixed = bytes
        .get(..INPUT_FIXED)
        .ok_or("native input frame is truncated")?;
    if fixed[..4] != INPUT_MAGIC {
        return Err("native input frame has no v2 call prefix".into());
    }
    let call = u64::from_le_bytes(fixed[4..12].try_into().unwrap());
    let length = u16::from_le_bytes(fixed[12..14].try_into().unwrap()) as usize;
    if length > MAX_BOUNDARY {
        return Err("native input boundary is too long".into());
    }
    let name = bytes
        .get(INPUT_FIXED..INPUT_FIXED + length)
        .ok_or("native input frame is truncated")?;
    let boundary =
        std::str::from_utf8(name).map_err(|_| "native input boundary is not UTF-8")?;
    Ok(Input {
        call,
        boundary,
        body: &bytes[INPUT_FIXED + length..],
    })
}

/// Fill an output prefix of exactly [`OUTPUT_PREFIX`] bytes with the call's
/// status and per-reason totals. Fails if any copy is still in flight.
pub(in crate::multiprocess) fn write_output(
    destination: &mut [MaybeUninit<u8>],
    call: u64,
    ok: bool,
    counts: &[CopyCounts; COPY_REASONS.len()],
) -> Result<(), String> {
    if destination.len() != OUTPUT_PREFIX {
        return Err("native output prefix length mismatch".into());
    }
    let mut prefix = [0u8; OUTPUT_PREFIX];
    prefix[..4].copy_from_slice(&OUTPUT_MAGIC);
    prefix[4..12].copy_from_slice(&call.to_le_bytes());
    prefix[12] = u8::from(!ok);
    prefix[13] = COPY_REASONS.len() as u8;
    for (index, counts) in counts.iter().enumerate() {
        if counts.in_flight != 0 {
            return Err("native call finished with copies in flight".into());
        }
        if counts.saturated {
            return Err("native call copy counts saturated".into());
        }
        let fields = [
            counts.completed,
            counts.copied_bytes,
            counts.failed,
            counts.failed_requested_bytes,
            counts.rejected,
            counts.rejected_bytes,
        ];
        for (field, value) in fields.into_iter().enumerate() {
            let at = OUTPUT_FIXED + (index * COUNT_FIELDS + field) * 8;
            prefix[at..at + 8].copy_from_slice(&value.to_le_bytes());
        }
    }
    put(destination, &prefix);
    Ok(())
}

/// One reason's totals for a call, validated for coherent replay.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(in crate::multiprocess) struct Totals {
    completed: u64,
    copied_bytes: u64,
    failed: u64,
    failed_bytes: u64,
    rejected: u64,
    rejected_bytes: u64,
}

/// A parsed, validated output frame borrowing the sample payload.
pub(in crate::multiprocess) struct Output<'a> {
    pub(in crate::multiprocess) call: u64,
    pub(in crate::multiprocess) ok: bool,
    pub(in crate::multiprocess) totals: [Totals; COPY_REASONS.len()],
    pub(in crate::multiprocess) body: &'a [u8],
}

pub(in crate::multiprocess) fn parse_output(bytes: &[u8]) -> Result<Output<'_>, String> {
    let prefix = bytes
        .get(..OUTPUT_PREFIX)
        .ok_or("native output frame is truncated")?;
    if prefix[..4] != OUTPUT_MAGIC {
        return Err("native output frame has no v2 call prefix".into());
    }
    let ok = match prefix[12] {
        0 => true,
        1 => false,
        _ => return Err("native output frame has an invalid status".into()),
    };
    if prefix[13] as usize != COPY_REASONS.len() || prefix[14..16] != [0, 0] {
        return Err("native output frame has an incompatible layout".into());
    }
    let field = |index: usize, field: usize| {
        let at = OUTPUT_FIXED + (index * COUNT_FIELDS + field) * 8;
        u64::from_le_bytes(prefix[at..at + 8].try_into().unwrap())
    };
    let mut totals = [Totals::default(); COPY_REASONS.len()];
    for (index, slot) in totals.iter_mut().enumerate() {
        *slot = Totals {
            completed: field(index, 0),
            copied_bytes: field(index, 1),
            failed: field(index, 2),
            failed_bytes: field(index, 3),
            rejected: field(index, 4),
            rejected_bytes: field(index, 5),
        };
        for (operations, bytes) in [
            (slot.completed, slot.copied_bytes),
            (slot.failed, slot.failed_bytes),
            (slot.rejected, slot.rejected_bytes),
        ] {
            if operations > MAX_OPERATIONS || (operations == 0 && bytes != 0) {
                return Err("native output frame has incoherent copy counts".into());
            }
        }
    }
    Ok(Output {
        call: u64::from_le_bytes(prefix[4..12].try_into().unwrap()),
        ok,
        totals,
        body: &bytes[OUTPUT_PREFIX..],
    })
}

/// Replay a call's validated totals into the caller's ledger. Ledgers keep
/// per-reason sums, so replaying `n` operations with all bytes on the first
/// yields exactly the counts the worker recorded.
pub(in crate::multiprocess) fn replay(copies: &CopyContext, totals: &[Totals; COPY_REASONS.len()]) {
    for (reason, totals) in COPY_REASONS.iter().zip(totals) {
        for index in 0..totals.completed {
            copies.record(*reason, CopyEvent::Started);
            let bytes = if index == 0 { totals.copied_bytes } else { 0 };
            copies.record(*reason, CopyEvent::Completed(bytes));
        }
        for index in 0..totals.failed {
            copies.record(*reason, CopyEvent::Started);
            let bytes = if index == 0 { totals.failed_bytes } else { 0 };
            copies.record(*reason, CopyEvent::Failed(bytes));
        }
        for index in 0..totals.rejected {
            let bytes = if index == 0 { totals.rejected_bytes } else { 0 };
            copies.record(*reason, CopyEvent::Rejected(bytes));
        }
    }
}

/// A granted call ended without the worker's terminal frame (worker exit,
/// protocol error or host stop). The host cannot see copies the worker had in
/// flight, so the call is recorded as one failed worker deserialization of
/// unknown size: never as completed work or as proof of zero copied bytes.
pub(in crate::multiprocess) fn record_interrupted(copies: &CopyContext) {
    copies.record(CopyReason::PortableDeserialization, CopyEvent::Started);
    copies.record(CopyReason::PortableDeserialization, CopyEvent::Failed(0));
}

fn put(destination: &mut [MaybeUninit<u8>], bytes: &[u8]) {
    assert_eq!(destination.len(), bytes.len());
    for (out, byte) in destination.iter_mut().zip(bytes) {
        out.write(*byte);
    }
}
