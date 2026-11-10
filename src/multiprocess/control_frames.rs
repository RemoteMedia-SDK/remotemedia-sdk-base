//! Bounded metadata framing shared by worker accounting protocols.
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Read, Write};
pub(crate) const MAX_FRAME: usize = 4096;

pub(crate) fn read_frame<T: DeserializeOwned>(stream: &mut impl Read) -> io::Result<T> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header) as usize;
    if size == 0 || size > MAX_FRAME {
        return Err(invalid("invalid copy frame size"));
    }
    // Frames are bounded, so a stack buffer avoids a heap allocation per frame.
    let mut buffer = [0u8; MAX_FRAME];
    let bytes = &mut buffer[..size];
    stream.read_exact(bytes)?;
    serde_json::from_slice(bytes).map_err(|_| invalid("invalid copy report schema"))
}

pub(crate) fn write_frame(stream: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    // Serialize after a 4-byte length prefix in one bounded stack buffer: no
    // heap growth per frame and a single write. Overflow means "too large".
    let mut buffer = [0u8; 4 + MAX_FRAME];
    let mut cursor = io::Cursor::new(&mut buffer[4..]);
    serde_json::to_writer(&mut cursor, value).map_err(|error| {
        if error.is_io() {
            invalid("copy reply too large")
        } else {
            invalid("invalid copy reply")
        }
    })?;
    let size = cursor.position() as usize;
    buffer[..4].copy_from_slice(&(size as u32).to_be_bytes());
    stream.write_all(&buffer[..4 + size])
}

pub(crate) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_with_length_prefix() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &serde_json::json!({"call": 7, "bytes": 3840})).unwrap();
        let size = u32::from_be_bytes(wire[..4].try_into().unwrap()) as usize;
        assert_eq!(size, wire.len() - 4);
        let value: serde_json::Value = read_frame(&mut wire.as_slice()).unwrap();
        assert_eq!(value, serde_json::json!({"call": 7, "bytes": 3840}));
    }

    #[test]
    fn oversized_and_empty_frames_are_rejected() {
        let mut wire = Vec::new();
        let error = write_frame(&mut wire, &"x".repeat(MAX_FRAME)).unwrap_err();
        assert_eq!(error.to_string(), "copy reply too large");
        assert!(wire.is_empty(), "nothing is written for a rejected frame");

        for size in [0u32, MAX_FRAME as u32 + 1] {
            let header = size.to_be_bytes();
            let error = read_frame::<serde_json::Value>(&mut header.as_slice()).unwrap_err();
            assert_eq!(error.to_string(), "invalid copy frame size");
        }
    }
}
