//! Pack visible native rows into the caller-selected output loan.
use super::codec::{CodecError, Result};
use crate::data::video::{validate_plane_span, PixelFormat};
use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrame, VideoFrameScaler};
use remotemedia_types::{CopyContext, CopyReason, OutputAllocator, SharedBytes};

struct Layout {
    name: &'static str,
    planes: Vec<(usize, usize)>,
    bytes: usize,
}

impl Layout {
    fn new(width: usize, height: usize, format: PixelFormat) -> Result<Self> {
        if width == 0 || height == 0 || width > i32::MAX as usize || height > i32::MAX as usize {
            return Err(CodecError::OutputLayout(
                "invalid native frame dimensions".into(),
            ));
        }
        let (name, planes) = match format {
            PixelFormat::Rgb24 => ("rgb24", vec![(width.checked_mul(3), height)]),
            PixelFormat::Rgba32 => ("rgba", vec![(width.checked_mul(4), height)]),
            PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12 => {
                if width % 2 != 0 || height % 2 != 0 {
                    return Err(CodecError::OutputLayout(
                        "shared 4:2:0 output requires even dimensions".into(),
                    ));
                }
                if format == PixelFormat::NV12 {
                    (
                        "nv12",
                        vec![(Some(width), height), (Some(width), height / 2)],
                    )
                } else {
                    (
                        "yuv420p",
                        vec![
                            (Some(width), height),
                            (Some(width / 2), height / 2),
                            (Some(width / 2), height / 2),
                        ],
                    )
                }
            }
            _ => {
                return Err(CodecError::OutputLayout(
                    "decoder output must have a supported raw pixel format".into(),
                ))
            }
        };
        let mut bytes = 0usize;
        let mut checked = Vec::with_capacity(planes.len());
        for (columns, rows) in planes {
            let columns =
                columns.ok_or_else(|| CodecError::OutputLayout("plane width overflow".into()))?;
            bytes = columns
                .checked_mul(rows)
                .and_then(|n| bytes.checked_add(n))
                .filter(|&n| n <= isize::MAX as usize)
                .ok_or_else(|| CodecError::OutputLayout("frame extent overflow".into()))?;
            checked.push((columns, rows));
        }
        Ok(Self {
            name,
            planes: checked,
            bytes,
        })
    }
}

pub(super) fn decoder_frame(
    frame: &VideoFrame,
    format: PixelFormat,
    copies: &CopyContext,
    allocator: &dyn OutputAllocator,
) -> Result<SharedBytes> {
    let layout = Layout::new(frame.width(), frame.height(), format)?;
    // This safe binding exposes separate planes, not their shared AVBuffer or a
    // caller-owned conversion destination. Never infer common ownership from
    // adjacent pointers. Authorize before conversion, allocation or payload reads.
    let transfer = copies.begin(CopyReason::ForeignPoolIngress, layout.bytes as u64)?;
    let mut output = allocator.allocate_bytes(layout.bytes)?;
    if output.len() != layout.bytes {
        return Err(CodecError::OutputLayout(
            "allocator returned an incorrect output extent".into(),
        ));
    }
    let converted = convert(frame, layout.name)?;
    let frame = converted.as_ref().unwrap_or(frame);
    let planes = frame.planes();
    // Check all planes before writing any output. Only touch planes valid for
    // the concrete converted format; unused native planes may be null.
    for (plane, &(columns, rows)) in planes.iter().zip(&layout.planes) {
        if plane.line_count() < rows {
            return Err(CodecError::OutputLayout(
                "native plane has too few rows".into(),
            ));
        }
        // ac-ffmpeg constructs slices from stride * row count. Check that
        // multiplication before calling data()/line(), including negative native
        // strides represented as usize by the binding.
        let extent = plane
            .line_size()
            .checked_mul(plane.line_count())
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| CodecError::OutputLayout("native plane extent overflow".into()))?;
        validate_plane_span(extent, plane.line_size(), columns, rows)
            .map_err(|e| CodecError::OutputLayout(e.to_string()))?;
    }
    let mut offset = 0;
    for (plane, &(columns, rows)) in planes.iter().zip(&layout.planes) {
        for row in 0..rows {
            output[offset..offset + columns].copy_from_slice(&plane.line(row).unwrap()[..columns]);
            offset += columns;
        }
    }
    transfer.complete();
    Ok(output.freeze())
}

fn convert(frame: &VideoFrame, name: &str) -> Result<Option<VideoFrame>> {
    let target = get_pixel_format(name);
    if frame.pixel_format() == target {
        return Ok(None);
    }
    let mut scaler = VideoFrameScaler::builder()
        .source_pixel_format(frame.pixel_format())
        .source_width(frame.width())
        .source_height(frame.height())
        .target_pixel_format(target)
        .target_width(frame.width())
        .target_height(frame.height())
        .build()
        .map_err(|e| CodecError::OutputLayout(format!("cannot configure pixel conversion: {e}")))?;
    scaler
        .scale(frame)
        .map(Some)
        .map_err(|e| CodecError::OutputLayout(format!("pixel conversion failed: {e}")))
}

#[cfg(test)]
#[path = "decoded_frames_tests.rs"]
mod tests;
