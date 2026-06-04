//! Video codec data types and utilities
//!
//! Re-exports the wire-format `PixelFormat` and `VideoCodec` enums
//! from `remotemedia-types` and provides core-only colour-space
//! conversion helpers (RGB24/RGBA32 → YUV420p) used by the Y4M
//! file writer and the VP8/H264/AV1 encoder.
//!
//! See spec 012: Video Codec Support (AV1/VP8/AVC).

pub use remotemedia_types::{PixelFormat, VideoCodec};

/// BT.601 RGB24 → YUV420p (planar) conversion. Y is full
/// resolution; Cb / Cr are 2×2 sub-sampled. Padding rows / cols
/// for odd dimensions get the last valid row / col duplicated.
///
/// Output layout: `[Y_plane][Cb_plane][Cr_plane]`, total
/// `w*h + 2*((w+1)/2)*((h+1)/2)` bytes.
///
/// Used by both the Y4M file writer and the VP8/H264/AV1 encoder
/// so they convert the same way.
pub fn rgb24_to_yuv420p(rgb: &[u8], w: usize, h: usize) -> Vec<u8> {
    let y_size = w * h;
    let chroma_w = (w + 1) / 2;
    let chroma_h = (h + 1) / 2;
    let chroma_size = chroma_w * chroma_h;
    let mut out = vec![0u8; y_size + 2 * chroma_size];
    let (y_plane, rest) = out.split_at_mut(y_size);
    let (cb_plane, cr_plane) = rest.split_at_mut(chroma_size);

    for y in 0..h {
        for x in 0..w {
            let i = (y * w + x) * 3;
            let r = rgb[i] as f32;
            let g = rgb[i + 1] as f32;
            let b = rgb[i + 2] as f32;
            let y_val = 0.299 * r + 0.587 * g + 0.114 * b;
            y_plane[y * w + x] = y_val.round().clamp(0.0, 255.0) as u8;
        }
    }

    for cy in 0..chroma_h {
        for cx in 0..chroma_w {
            let mut r_sum = 0.0f32;
            let mut g_sum = 0.0f32;
            let mut b_sum = 0.0f32;
            let mut count = 0.0f32;
            for dy in 0..2 {
                for dx in 0..2 {
                    let py = (cy * 2 + dy).min(h - 1);
                    let px = (cx * 2 + dx).min(w - 1);
                    let i = (py * w + px) * 3;
                    r_sum += rgb[i] as f32;
                    g_sum += rgb[i + 1] as f32;
                    b_sum += rgb[i + 2] as f32;
                    count += 1.0;
                }
            }
            let r = r_sum / count;
            let g = g_sum / count;
            let b = b_sum / count;
            let cb = 128.0 - 0.168736 * r - 0.331264 * g + 0.5 * b;
            let cr = 128.0 + 0.5 * r - 0.418688 * g - 0.081312 * b;
            cb_plane[cy * chroma_w + cx] = cb.round().clamp(0.0, 255.0) as u8;
            cr_plane[cy * chroma_w + cx] = cr.round().clamp(0.0, 255.0) as u8;
        }
    }
    out
}

/// BT.601 RGBA32 → YUV420p, ignoring the alpha channel.
pub fn rgba32_to_yuv420p(rgba: &[u8], w: usize, h: usize) -> Vec<u8> {
    let y_size = w * h;
    let chroma_w = (w + 1) / 2;
    let chroma_h = (h + 1) / 2;
    let chroma_size = chroma_w * chroma_h;
    let mut out = vec![0u8; y_size + 2 * chroma_size];
    let (y_plane, rest) = out.split_at_mut(y_size);
    let (cb_plane, cr_plane) = rest.split_at_mut(chroma_size);

    for y in 0..h {
        for x in 0..w {
            let i = (y * w + x) * 4;
            let r = rgba[i] as f32;
            let g = rgba[i + 1] as f32;
            let b = rgba[i + 2] as f32;
            let y_val = 0.299 * r + 0.587 * g + 0.114 * b;
            y_plane[y * w + x] = y_val.round().clamp(0.0, 255.0) as u8;
        }
    }

    for cy in 0..chroma_h {
        for cx in 0..chroma_w {
            let mut r_sum = 0.0f32;
            let mut g_sum = 0.0f32;
            let mut b_sum = 0.0f32;
            let mut count = 0.0f32;
            for dy in 0..2 {
                for dx in 0..2 {
                    let py = (cy * 2 + dy).min(h - 1);
                    let px = (cx * 2 + dx).min(w - 1);
                    let i = (py * w + px) * 4;
                    r_sum += rgba[i] as f32;
                    g_sum += rgba[i + 1] as f32;
                    b_sum += rgba[i + 2] as f32;
                    count += 1.0;
                }
            }
            let r = r_sum / count;
            let g = g_sum / count;
            let b = b_sum / count;
            let cb = 128.0 - 0.168736 * r - 0.331264 * g + 0.5 * b;
            let cr = 128.0 + 0.5 * r - 0.418688 * g - 0.081312 * b;
            cb_plane[cy * chroma_w + cx] = cb.round().clamp(0.0, 255.0) as u8;
            cr_plane[cy * chroma_w + cx] = cr.round().clamp(0.0, 255.0) as u8;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pixel-format / codec enum behaviour is exercised in
    // `remotemedia-types`. Keep a smoke test here so the
    // re-export wiring is verified at the core layer too.
    #[test]
    fn re_exports_compile() {
        assert_eq!(PixelFormat::Rgb24.buffer_size(8, 8), 192);
        assert_eq!(VideoCodec::Vp8.mime_type(), "video/VP8");
    }
}
