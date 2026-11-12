//! Legacy Python wire compatibility. Mapped workers bypass this codec.
//! Bulk encoding writes once into the final wire buffer. Owned byte decoding
//! retains a slice; borrowed decoding and unaligned LE audio require a copy.
use remotemedia_types::{CopyContext, CopyReason, RuntimeData, SharedBytes};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireDataType {
    Audio = 1,
    Video = 2,
    Text = 3,
    Tensor = 4,
    ControlMessage = 5,
    Numpy = 6,
    File = 7,
    EndOfInput = 8,
    Json = 9,
}

/// Legacy entrypoint. Required bulk transfers use the unattributed ledger.
pub fn to_python_wire(data: &RuntimeData, session_id: &str) -> Result<Vec<u8>, String> {
    to_python_wire_with_copy_context(data, session_id, CopyContext::legacy())
}

/// Authorize before reading bulk data. Header/metadata allocation is separate
/// from payload transfer. No completed bulk payload is staged in a second Vec.
pub fn to_python_wire_with_copy_context(
    data: &RuntimeData,
    session_id: &str,
    copies: &CopyContext,
) -> Result<Vec<u8>, String> {
    if matches!(data, RuntimeData::Binary(_) | RuntimeData::Image { .. }) {
        return Err(
            "legacy Python wire cannot represent image/binary; use mapped execution".into(),
        );
    }
    let operation = copies
        .begin(
            CopyReason::PortableSerialization,
            data.bulk_payload_bytes() as u64,
        )
        .map_err(|e| e.to_string())?;
    let mut wire = vec![0];
    string16(&mut wire, session_id)?;
    let timestamp = match data {
        RuntimeData::Video { timestamp_us, .. } => *timestamp_us,
        _ => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_micros() as u64,
    };
    wire.extend_from_slice(&timestamp.to_le_bytes());
    let length_at = wire.len();
    wire.extend_from_slice(&0u32.to_le_bytes());
    let payload_at = wire.len();
    wire[0] = write_payload(&mut wire, data)? as u8;
    let length =
        u32::try_from(wire.len() - payload_at).map_err(|_| "Python payload exceeds u32")?;
    wire[length_at..payload_at].copy_from_slice(&length.to_le_bytes());
    operation.complete();
    Ok(wire)
}

// ── Turn tags (openspec: add-node-initiated-output) ───────────────────
//
// A frame may carry a tag after its payload. The payload length in the header
// is unchanged, so the tag is whatever follows the payload:
//
//   input  (host → worker): seq u64 | origin_len u16 | origin (JSON object of
//                           the sender's `participant.*` fields, or empty)
//   output (worker → host): seq u64 | origin_mode u8 | id_len u16 | id
//
// `seq` numbers the input a frame belongs to. An output with `seq` 0 answers
// no input: the node emitted it. A worker tags its outputs when its host set
// `REMOTEMEDIA_PY_WIRE_TURNS=1` in its environment; a host tags its inputs
// once the worker announced `TURNS:1`. Either side accepts untagged frames.

/// Environment variable a host sets to `1` on the workers it starts when it
/// reads turn tags. A host started with it set to `0` does not.
pub const TURN_TAG_ENV: &str = "REMOTEMEDIA_PY_WIRE_TURNS";

/// Whether this host reads turn tags (`REMOTEMEDIA_PY_WIRE_TURNS=0` opts out).
pub fn turn_tags_enabled() -> bool {
    std::env::var(TURN_TAG_ENV).map_or(true, |value| value != "0")
}

/// Environment variable a host sets to `1` on the workers it starts when it
/// sends them a start signal (openspec: add-source-node-lifecycle). A worker
/// whose node has a `background()` then announces `START:1` before `READY`
/// and starts the body when the signal arrives: a [`start_frame`] on its
/// input lane. Without the variable the worker starts the body after READY.
pub const START_ENV: &str = "REMOTEMEDIA_PY_WIRE_START";

/// Whether this host sends the start signal (`REMOTEMEDIA_PY_WIRE_START=0`
/// opts out: workers then start `background()` once their node is ready).
pub fn start_signal_enabled() -> bool {
    std::env::var(START_ENV).map_or(true, |value| value != "0")
}

/// Wire type of the start signal: a frame with no payload. Not a data type:
/// 1 to 9 are data and the end-of-input marker, 10 and 11 exist on the mapped
/// transport.
pub const START_FRAME_TYPE: u8 = 12;

/// The start signal for a worker of `session_id`.
pub fn start_frame(session_id: &str) -> Vec<u8> {
    let session = session_id.as_bytes();
    let mut frame = Vec::with_capacity(15 + session.len());
    frame.push(START_FRAME_TYPE);
    frame.extend_from_slice(&(session.len() as u16).to_le_bytes());
    frame.extend_from_slice(session);
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());
    frame
}

/// Who an emitted output frame is attributed to, as the worker asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitTag {
    /// The instance's default attribution.
    Default,
    /// Nobody.
    Unattributed,
    /// The participant with this id.
    Participant(String),
}

/// The tag of an output frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputTurn {
    /// The input this frame answers; 0 for an emitted frame.
    pub seq: u64,
    /// Attribution requested for an emitted frame.
    pub origin: EmitTag,
}

/// Offset one past the payload, when the header is well formed.
fn payload_end(frame: &[u8]) -> Option<usize> {
    let session = u16::from_le_bytes(frame.get(1..3)?.try_into().ok()?) as usize;
    let length_at = 3usize.checked_add(session)?.checked_add(8)?;
    let payload = u32::from_le_bytes(frame.get(length_at..length_at + 4)?.try_into().ok()?);
    let end = (length_at + 4).checked_add(payload as usize)?;
    (end <= frame.len()).then_some(end)
}

/// Tag an input frame with its sequence number and its sender's fields.
pub fn append_input_turn(
    wire: &mut Vec<u8>,
    seq: u64,
    origin: Option<&std::collections::HashMap<String, String>>,
) -> Result<(), String> {
    let origin = match origin {
        Some(fields) => serde_json::to_vec(fields).map_err(|e| e.to_string())?,
        None => Vec::new(),
    };
    let length = u16::try_from(origin.len()).map_err(|_| "participant fields exceed u16")?;
    wire.extend_from_slice(&seq.to_le_bytes());
    wire.extend_from_slice(&length.to_le_bytes());
    wire.extend_from_slice(&origin);
    Ok(())
}

/// Split an output frame into the frame proper and its tag. `None` for a
/// frame without a tag.
pub fn split_output_turn(frame: &SharedBytes) -> Result<Option<(SharedBytes, OutputTurn)>, String> {
    let bytes: &[u8] = frame;
    let Some(end) = payload_end(bytes) else {
        return Ok(None);
    };
    if end == bytes.len() {
        return Ok(None);
    }
    let mut tag = Reader {
        bytes: &bytes[end..],
        position: 0,
    };
    let seq = tag.u64()?;
    let mode = tag.u8()?;
    let id = tag.string16()?;
    if !tag.rest().is_empty() {
        return Err("trailing bytes after the turn tag".into());
    }
    let origin = match mode {
        0 => EmitTag::Default,
        1 => EmitTag::Unattributed,
        2 => EmitTag::Participant(id),
        other => return Err(format!("unknown emit origin mode {other}")),
    };
    let frame = frame.slice(0..end).map_err(|e| e.to_string())?;
    Ok(Some((frame, OutputTurn { seq, origin })))
}

#[cfg(test)]
mod turn_tag_tests {
    use super::*;

    fn text_frame(body: &str) -> Vec<u8> {
        to_python_wire(&RuntimeData::Text(body.into()), "session").unwrap()
    }

    #[test]
    fn an_untagged_frame_has_no_turn() {
        let frame = SharedBytes::from(text_frame("hello"));
        assert_eq!(split_output_turn(&frame).unwrap(), None);
    }

    #[test]
    fn an_output_tag_is_split_off_and_the_frame_still_decodes() {
        for (mode, id, expected) in [
            (0u8, "", EmitTag::Default),
            (1, "", EmitTag::Unattributed),
            (2, "alice", EmitTag::Participant("alice".into())),
        ] {
            let mut wire = text_frame("hello");
            wire.extend_from_slice(&7u64.to_le_bytes());
            wire.push(mode);
            wire.extend_from_slice(&(id.len() as u16).to_le_bytes());
            wire.extend_from_slice(id.as_bytes());
            let (frame, turn) = split_output_turn(&SharedBytes::from(wire)).unwrap().unwrap();
            assert_eq!(turn, OutputTurn { seq: 7, origin: expected });
            assert!(matches!(from_python_wire(&frame).unwrap(), RuntimeData::Text(t) if t == "hello"));
        }
    }

    #[test]
    fn a_malformed_tag_is_an_error() {
        let mut wire = text_frame("hello");
        wire.extend_from_slice(&[1, 2, 3]);
        assert!(split_output_turn(&SharedBytes::from(wire)).is_err());
    }

    #[test]
    fn an_input_tag_follows_the_payload() {
        let untagged = text_frame("hello");
        let mut wire = untagged.clone();
        let fields = std::collections::HashMap::from([("participant.id".to_string(), "bob".to_string())]);
        append_input_turn(&mut wire, 3, Some(&fields)).unwrap();
        assert_eq!(payload_end(&wire), Some(untagged.len()));
        let tag = &wire[untagged.len()..];
        assert_eq!(u64::from_le_bytes(tag[..8].try_into().unwrap()), 3);
        let length = u16::from_le_bytes(tag[8..10].try_into().unwrap()) as usize;
        let origin: serde_json::Value = serde_json::from_slice(&tag[10..10 + length]).unwrap();
        assert_eq!(origin["participant.id"], "bob");
    }
}

fn string16(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    let length = u16::try_from(value.len()).map_err(|_| "Python wire string exceeds u16")?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn json32(out: &mut Vec<u8>, value: &Option<Value>) -> Result<(), String> {
    let bytes = value
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or_default();
    let length = u32::try_from(bytes.len()).map_err(|_| "Python metadata exceeds u32")?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&bytes);
    Ok(())
}

fn write_payload(out: &mut Vec<u8>, value: &RuntimeData) -> Result<WireDataType, String> {
    use WireDataType as W;
    let (kind, bytes): (W, &[u8]) = match value {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            metadata,
            ..
        } => {
            let channels =
                u16::try_from(*channels).map_err(|_| "Python audio channels exceed u16")?;
            out.extend_from_slice(&sample_rate.to_le_bytes());
            out.extend_from_slice(&channels.to_le_bytes());
            json32(out, metadata)?;
            out.reserve(samples.len().checked_mul(4).ok_or("audio size overflow")?);
            for sample in samples.as_slice() {
                out.extend_from_slice(&sample.to_le_bytes());
            }
            return Ok(W::Audio);
        }
        RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            is_keyframe,
            ..
        } => {
            out.extend_from_slice(&width.to_le_bytes());
            out.extend_from_slice(&height.to_le_bytes());
            out.extend_from_slice(&[*format as u8, codec.map(|c| c as u8).unwrap_or(0)]);
            out.extend_from_slice(&frame_number.to_le_bytes());
            out.push(u8::from(*is_keyframe));
            (W::Video, pixel_data)
        }
        RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata,
        } => {
            out.push(u8::try_from(shape.len()).map_err(|_| "Python tensor rank exceeds u8")?);
            for dimension in shape {
                let dimension =
                    u32::try_from(*dimension).map_err(|_| "negative tensor dimension")?;
                out.extend_from_slice(&dimension.to_le_bytes());
            }
            let dtype = u8::try_from(*dtype).map_err(|_| "Python tensor dtype exceeds u8")?;
            tensor_size(shape, dtype, data.len())?;
            out.push(dtype);
            json32(out, metadata)?;
            (W::Tensor, data)
        }
        RuntimeData::Numpy {
            data,
            shape,
            dtype,
            strides,
            c_contiguous,
            f_contiguous,
        } => {
            out.extend_from_slice(
                &u32::try_from(dtype.len())
                    .map_err(|_| "dtype length overflow")?
                    .to_le_bytes(),
            );
            out.extend_from_slice(dtype.as_bytes());
            out.extend_from_slice(
                &u32::try_from(shape.len())
                    .map_err(|_| "shape rank overflow")?
                    .to_le_bytes(),
            );
            for dim in shape {
                out.extend_from_slice(
                    &u32::try_from(*dim)
                        .map_err(|_| "shape overflow")?
                        .to_le_bytes(),
                );
            }
            out.extend_from_slice(
                &u32::try_from(strides.len())
                    .map_err(|_| "strides rank overflow")?
                    .to_le_bytes(),
            );
            for stride in strides {
                out.extend_from_slice(
                    &u32::try_from(*stride)
                        .map_err(|_| "unsupported stride")?
                        .to_le_bytes(),
                );
            }
            out.extend_from_slice(&[u8::from(*c_contiguous), u8::from(*f_contiguous)]);
            (W::Numpy, data)
        }
        RuntimeData::Text(text) => (W::Text, text.as_bytes()),
        RuntimeData::Json(value) => {
            serde_json::to_writer(out, value).map_err(|e| e.to_string())?;
            return Ok(W::Json);
        }
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            serde_json::to_writer(
                out,
                &serde_json::json!({
                    "message_type": message_type, "segment_id": segment_id,
                    "timestamp_ms": timestamp_ms, "metadata": metadata,
                }),
            )
            .map_err(|e| e.to_string())?;
            return Ok(W::ControlMessage);
        }
        RuntimeData::File {
            path,
            filename,
            mime_type,
            size,
            offset,
            length,
            stream_id,
        } => {
            string16(out, path)?;
            string16(out, filename.as_deref().unwrap_or(""))?;
            string16(out, mime_type.as_deref().unwrap_or(""))?;
            for number in [size, offset, length] {
                out.extend_from_slice(&number.unwrap_or(0).to_le_bytes());
            }
            string16(out, stream_id.as_deref().unwrap_or(""))?;
            return Ok(W::File);
        }
        _ => return Err("unsupported Python wire payload".into()),
    };
    out.reserve(bytes.len());
    out.extend_from_slice(bytes);
    Ok(kind)
}

fn tensor_size(shape: &[i32], dtype: u8, bytes: usize) -> Result<(), String> {
    let width = match dtype {
        1 | 3 => 4usize,
        2 | 4 => 8,
        5 => 1,
        6 => 2,
        _ => return Err("unsupported Python tensor dtype".into()),
    };
    let expected = shape.iter().try_fold(width, |size, dim| {
        let dim = usize::try_from(*dim).map_err(|_| "negative tensor dimension")?;
        size.checked_mul(dim).ok_or("tensor size overflow")
    })?;
    if expected != bytes {
        return Err("tensor shape/dtype does not match payload length".into());
    }
    Ok(())
}

pub fn from_python_wire(bytes: &[u8]) -> Result<RuntimeData, String> {
    from_python_wire_with_copy_context(bytes, CopyContext::legacy())
}

pub fn from_python_wire_with_copy_context(
    bytes: &[u8],
    copies: &CopyContext,
) -> Result<RuntimeData, String> {
    decode(bytes, None, copies)
}

/// Retain non-audio bulk slices in the received owner. Audio is unaligned LE
/// wire storage, so conversion to native F32 is explicitly authorized/counted.
pub fn from_owned_python_wire_with_copy_context(
    bytes: SharedBytes,
    copies: &CopyContext,
) -> Result<RuntimeData, String> {
    decode(&bytes, Some(&bytes), copies)
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self
            .position
            .checked_add(count)
            .ok_or("wire length overflow")?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or("truncated Python wire field")?;
        self.position = end;
        Ok(result)
    }
    fn number<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "invalid integer width".into())
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.number::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.number()?))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.number()?))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.number()?))
    }
    fn string(&mut self, count: usize) -> Result<String, String> {
        String::from_utf8(self.take(count)?.to_vec()).map_err(|e| e.to_string())
    }
    fn string16(&mut self) -> Result<String, String> {
        let size = self.u16()? as usize;
        self.string(size)
    }
    fn json32(&mut self) -> Result<Option<Value>, String> {
        let size = self.u32()? as usize;
        if size == 0 {
            return Ok(None);
        }
        serde_json::from_slice(self.take(size)?)
            .map(Some)
            .map_err(|e| e.to_string())
    }
    fn dimensions(&mut self, count: usize) -> Result<Vec<u32>, String> {
        let size = count.checked_mul(4).ok_or("dimension count overflow")?;
        Ok(self
            .take(size)?
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }
    fn bulk(
        &self,
        owner: Option<&SharedBytes>,
        copies: &CopyContext,
    ) -> Result<SharedBytes, String> {
        match owner {
            Some(owner) => owner
                .slice(self.position..self.bytes.len())
                .map_err(|e| e.to_string()),
            None => copies
                .materialize(
                    CopyReason::PortableDeserialization,
                    self.rest().len() as u64,
                    || self.rest().to_vec().into(),
                )
                .map_err(|e| e.to_string()),
        }
    }
}

fn decode(
    bytes: &[u8],
    owner: Option<&SharedBytes>,
    copies: &CopyContext,
) -> Result<RuntimeData, String> {
    let mut r = Reader { bytes, position: 0 };
    let kind = r.u8()?;
    r.string16()?;
    let timestamp = r.u64()?;
    let size = r.u32()? as usize;
    if size != r.rest().len() {
        return Err("Python wire payload length mismatch".into());
    }
    match kind {
        1 => {
            let sample_rate = r.u32()?;
            let channels = r.u16()? as u32;
            let metadata = r.json32()?;
            let data = r.rest();
            if data.len() % 4 != 0 {
                return Err("partial F32 audio sample".into());
            }
            let samples = copies
                .materialize(
                    CopyReason::PortableDeserialization,
                    data.len() as u64,
                    || {
                        data.chunks_exact(4)
                            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                            .collect::<Vec<_>>()
                    },
                )
                .map_err(|e| e.to_string())?;
            Ok(RuntimeData::Audio {
                samples: samples.into(),
                sample_rate,
                channels,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata,
            })
        }
        2 => {
            let width = r.u32()?;
            let height = r.u32()?;
            use remotemedia_types::{PixelFormat as P, VideoCodec as C};
            let format = match r.u8()? {
                0 => P::Unspecified,
                1 => P::Yuv420p,
                2 => P::I420,
                3 => P::NV12,
                4 => P::Rgb24,
                5 => P::Rgba32,
                255 => P::Encoded,
                _ => return Err("invalid video format".into()),
            };
            let codec = match r.u8()? {
                0 => None,
                1 => Some(C::Vp8),
                2 => Some(C::H264),
                3 => Some(C::Av1),
                _ => return Err("invalid video codec".into()),
            };
            let frame_number = r.u64()?;
            let is_keyframe = r.u8()? != 0;
            Ok(RuntimeData::Video {
                pixel_data: r.bulk(owner, copies)?,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us: timestamp,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            })
        }
        3 => {
            let text = std::str::from_utf8(r.rest())
                .map_err(|e| e.to_string())?
                .to_owned();
            Ok(serde_json::from_str(&text)
                .map(RuntimeData::Json)
                .unwrap_or(RuntimeData::Text(text)))
        }
        4 => {
            let rank = r.u8()? as usize;
            let shape = r
                .dimensions(rank)?
                .into_iter()
                .map(|d| i32::try_from(d).map_err(|_| "tensor dimension exceeds i32"))
                .collect::<Result<Vec<_>, _>>()?;
            let dtype = r.u8()?;
            let metadata = r.json32()?;
            tensor_size(&shape, dtype, r.rest().len())?;
            Ok(RuntimeData::Tensor {
                data: r.bulk(owner, copies)?,
                shape,
                dtype: dtype.into(),
                metadata,
            })
        }
        5 => {
            let value: Value = serde_json::from_slice(r.rest()).map_err(|e| e.to_string())?;
            let message_type = control_type(&value)?;
            Ok(RuntimeData::ControlMessage {
                message_type,
                segment_id: value
                    .get("segment_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                timestamp_ms: value
                    .get("timestamp_ms")
                    .and_then(Value::as_u64)
                    .ok_or("invalid control timestamp")?,
                metadata: value.get("metadata").cloned().unwrap_or(Value::Null),
            })
        }
        6 => {
            let size = r.u32()? as usize;
            let dtype = r.string(size)?;
            let rank = r.u32()? as usize;
            let shape = r
                .dimensions(rank)?
                .into_iter()
                .map(|v| v as usize)
                .collect();
            let rank = r.u32()? as usize;
            let strides = r
                .dimensions(rank)?
                .into_iter()
                .map(|v| isize::try_from(v).map_err(|_| "stride exceeds isize"))
                .collect::<Result<Vec<_>, _>>()?;
            let c_contiguous = r.u8()? != 0;
            let f_contiguous = r.u8()? != 0;
            Ok(RuntimeData::Numpy {
                data: r.bulk(owner, copies)?,
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
            })
        }
        7 => {
            let path = r.string16()?;
            let filename = nonempty(r.string16()?);
            let mime_type = nonempty(r.string16()?);
            let size = nonzero(r.u64()?);
            let offset = nonzero(r.u64()?);
            let length = nonzero(r.u64()?);
            let stream_id = nonempty(r.string16()?);
            if !r.rest().is_empty() {
                return Err("trailing file reference bytes".into());
            }
            Ok(RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            })
        }
        8 if r.rest().is_empty() => Ok(RuntimeData::Text(String::new())),
        9 => serde_json::from_slice(r.rest())
            .map(RuntimeData::Json)
            .map_err(|e| e.to_string()),
        _ => Err(format!("unsupported Python wire data type {kind}")),
    }
}

fn nonempty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
fn nonzero(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

// Accept both Python's external tag and the historical flat/internal envelopes.
fn control_type(value: &Value) -> Result<remotemedia_types::ControlMessageType, String> {
    let value = value.get("message_type").unwrap_or(value);
    if let Ok(kind) = serde_json::from_value(value.clone()) {
        return Ok(kind);
    }
    let (name, fields) = match value.get("type").and_then(Value::as_str) {
        Some(name) => (name, value),
        None => {
            let object = value.as_object().ok_or("invalid control message type")?;
            if object.len() != 1 {
                return Err("ambiguous control message type".into());
            }
            let (name, fields) = object.iter().next().unwrap();
            (name.as_str(), fields)
        }
    };
    let name = match name {
        "cancel_speculation" | "CancelSpeculation" => "CancelSpeculation",
        "batch_hint" | "BatchHint" => "BatchHint",
        "deadline_warning" | "DeadlineWarning" => "DeadlineWarning",
        _ => return Err("unknown control message type".into()),
    };
    let mut fields = fields.as_object().ok_or("invalid control fields")?.clone();
    fields.insert("type".into(), Value::String(name.into()));
    serde_json::from_value(Value::Object(fields)).map_err(|e| e.to_string())
}
