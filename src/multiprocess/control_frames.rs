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
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid copy report schema"))
}

pub(crate) fn write_frame(stream: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("invalid copy reply"))?;
    if bytes.len() > MAX_FRAME {
        return Err(invalid("copy reply too large"));
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)
}

pub(crate) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
