//! Explicit raw-frame transfers into FFmpeg's strided, backend-owned planes.
use super::codec::{CodecError, Result};
use crate::data::video::{rgb_to_yuv420p_planes, validate_plane_span, yuv420p_size, PixelFormat};
use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrameMut};
use remotemedia_types::{CopyContext, CopyReason};

/// Validate before allocation, then compute RGB directly or transfer planar YUV.
/// Backend-private allocation is not a selected mapped output-provider binding.
pub(super) fn encoder_frame(
    data: &[u8],
    width: u32,
    height: u32,
    format: PixelFormat,
    copies: &CopyContext,
) -> Result<VideoFrameMut> {
    if width == 0
        || height == 0
        || width > i32::MAX as u32
        || height > i32::MAX as u32
        || width % 2 != 0
        || height % 2 != 0
    {
        return Err(CodecError::InvalidInput(
            "encoder requires positive, even dimensions within native integer limits".into(),
        ));
    }
    let (w, h) = (width as usize, height as usize);
    let yuv_size = yuv420p_size(w, h).map_err(|e| CodecError::InvalidInput(e.to_string()))?;
    let channels = match format {
        PixelFormat::Yuv420p | PixelFormat::I420 => None,
        PixelFormat::Rgb24 => Some(3),
        PixelFormat::Rgba32 => Some(4),
        _ => {
            return Err(CodecError::InvalidInput(format!(
                "unsupported encoder pixel format: {format:?}"
            )))
        }
    };
    let expected = channels.map_or(Some(yuv_size), |n| w.checked_mul(h)?.checked_mul(n));
    if expected != Some(data.len()) {
        return Err(CodecError::InvalidInput(
            "frame buffer size mismatch: span length differs from dimensions".into(),
        ));
    }
    // Authorize the unavoidable planar ownership transfer before allocating or writing.
    let transfer = if channels.is_none() {
        Some(copies.begin(CopyReason::ForeignPoolIngress, yuv_size as u64)?)
    } else {
        None // Color conversion computes output, with no intermediate payload buffer.
    };
    let mut frame = VideoFrameMut::black(get_pixel_format("yuv420p"), w, h);
    let mut planes = frame.planes_mut();
    let (y, uv) = planes.split_at_mut(1);
    let (u, v) = uv.split_at_mut(1);
    let strides = [y[0].line_size(), u[0].line_size(), v[0].line_size()];
    let spans = [
        (y[0].data_mut(), strides[0]),
        (u[0].data_mut(), strides[1]),
        (v[0].data_mut(), strides[2]),
    ];
    if let Some(channels) = channels {
        rgb_to_yuv420p_planes(data, w, h, channels, spans)
            .map_err(|e| CodecError::InvalidInput(e.to_string()))?;
    } else {
        copy_yuv_planes(data, w, h, spans)?;
    }
    if let Some(transfer) = transfer {
        transfer.complete();
    }
    drop(planes);
    Ok(frame)
}

fn copy_yuv_planes(data: &[u8], w: usize, h: usize, planes: [(&mut [u8], usize); 3]) -> Result<()> {
    let shapes = [(w, h), (w / 2, h / 2), (w / 2, h / 2)];
    for ((plane, stride), (columns, rows)) in planes.iter().zip(shapes) {
        validate_plane_span(plane.len(), *stride, columns, rows)
            .map_err(|e| CodecError::InvalidInput(e.to_string()))?;
    }
    let mut offset = 0;
    for ((plane, stride), (columns, rows)) in planes.into_iter().zip(shapes) {
        for row in 0..rows {
            plane[row * stride..row * stride + columns]
                .copy_from_slice(&data[offset..offset + columns]);
            offset += columns;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "frames_tests.rs"]
mod tests;
