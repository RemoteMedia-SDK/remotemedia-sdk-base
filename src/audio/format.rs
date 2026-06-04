//! Audio format conversion utilities
//!
//! Provides efficient conversions between different audio sample formats:
//! - I16 ↔ F32: Integer to floating point conversion
//! - I32 ↔ F32: High-precision integer to floating point
//! - Zero-copy transmutes where possible using bytemuck

/// Convert i16 samples to f32 (range: -32768..32767 → -1.0..1.0)
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::i16_to_f32;
///
/// let i16_samples = vec![0, 16384, 32767, -16384, -32768];
/// let f32_samples = i16_to_f32(&i16_samples);
///
/// assert_eq!(f32_samples[0], 0.0);
/// assert!((f32_samples[2] - 1.0).abs() < 0.0001);
/// assert!((f32_samples[4] + 1.0).abs() < 0.0001);
/// ```
pub fn i16_to_f32(samples: &[i16]) -> Vec<f32> {
    samples.iter().map(|&s| s as f32 / 32768.0).collect()
}

/// Convert f32 samples to i16 (range: -1.0..1.0 → -32768..32767)
///
/// Values outside the range are clamped.
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::f32_to_i16;
///
/// let f32_samples = vec![0.0, 0.5, 1.0, -0.5, -1.0];
/// let i16_samples = f32_to_i16(&f32_samples);
///
/// assert_eq!(i16_samples[0], 0);
/// assert_eq!(i16_samples[2], 32767);
/// assert_eq!(i16_samples[4], -32768);
/// ```
pub fn f32_to_i16(samples: &[f32]) -> Vec<i16> {
    samples
        .iter()
        .map(|&s| {
            let clamped = s.clamp(-1.0, 1.0);
            if clamped >= 0.0 {
                (clamped * 32767.0) as i16
            } else {
                (clamped * 32768.0) as i16
            }
        })
        .collect()
}

/// Convert i32 samples to f32 (range: -2147483648..2147483647 → -1.0..1.0)
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::i32_to_f32;
///
/// let i32_samples = vec![0, 1073741824, 2147483647, -1073741824, -2147483648];
/// let f32_samples = i32_to_f32(&i32_samples);
///
/// assert_eq!(f32_samples[0], 0.0);
/// assert!((f32_samples[2] - 1.0).abs() < 0.0001);
/// assert!((f32_samples[4] + 1.0).abs() < 0.0001);
/// ```
pub fn i32_to_f32(samples: &[i32]) -> Vec<f32> {
    samples.iter().map(|&s| s as f32 / 2147483648.0).collect()
}

/// Convert f32 samples to i32 (range: -1.0..1.0 → -2147483648..2147483647)
///
/// Values outside the range are clamped.
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::f32_to_i32;
///
/// let f32_samples = vec![0.0, 0.5, 1.0, -0.5, -1.0];
/// let i32_samples = f32_to_i32(&f32_samples);
///
/// assert_eq!(i32_samples[0], 0);
/// assert_eq!(i32_samples[2], 2147483647);
/// assert_eq!(i32_samples[4], -2147483648);
/// ```
pub fn f32_to_i32(samples: &[f32]) -> Vec<i32> {
    samples
        .iter()
        .map(|&s| {
            let clamped = s.clamp(-1.0, 1.0);
            (clamped * 2147483647.0) as i32
        })
        .collect()
}

/// Zero-copy transmute from u8 slice to f32 slice (if alignment permits)
///
/// This is safe only if:
/// - The byte slice length is a multiple of 4
/// - The byte slice is properly aligned for f32
///
/// Returns None if the transmute is not safe.
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::transmute_u8_to_f32;
///
/// let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 128, 63]; // [0.0, 1.0] in LE
/// let f32_slice = transmute_u8_to_f32(&bytes);
///
/// assert!(f32_slice.is_some());
/// ```
pub fn transmute_u8_to_f32(bytes: &[u8]) -> Option<&[f32]> {
    // Check length alignment
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        return None;
    }

    // Check pointer alignment
    if (bytes.as_ptr() as usize) % std::mem::align_of::<f32>() != 0 {
        return None;
    }

    // Safe transmute using bytemuck
    Some(bytemuck::cast_slice(bytes))
}

/// Zero-copy transmute from f32 slice to u8 slice
///
/// Always safe since f32 is Pod.
///
/// # Example
/// ```
/// use remotemedia_core::audio::format::transmute_f32_to_u8;
///
/// let samples = vec![0.0_f32, 1.0];
/// let bytes = transmute_f32_to_u8(&samples);
///
/// assert_eq!(bytes.len(), 8); // 2 floats * 4 bytes each
/// ```
pub fn transmute_f32_to_u8(samples: &[f32]) -> &[u8] {
    bytemuck::cast_slice(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i16_to_f32_conversion() {
        let i16_samples = vec![0, 16384, 32767, -16384, -32768];
        let f32_samples = i16_to_f32(&i16_samples);

        assert_eq!(f32_samples.len(), 5);
        assert_eq!(f32_samples[0], 0.0);
        assert!((f32_samples[1] - 0.5).abs() < 0.01); // ~0.5
        assert!((f32_samples[2] - 1.0).abs() < 0.0001); // ~1.0
        assert!((f32_samples[3] + 0.5).abs() < 0.01); // ~-0.5
        assert!((f32_samples[4] + 1.0).abs() < 0.0001); // ~-1.0
    }

    #[test]
    fn test_f32_to_i16_conversion() {
        let f32_samples = vec![0.0, 0.5, 1.0, -0.5, -1.0];
        let i16_samples = f32_to_i16(&f32_samples);

        assert_eq!(i16_samples.len(), 5);
        assert_eq!(i16_samples[0], 0);
        assert!((i16_samples[1] - 16383).abs() <= 1); // ~16384
        assert_eq!(i16_samples[2], 32767);
        assert!((i16_samples[3] + 16383).abs() <= 1); // ~-16384
        assert_eq!(i16_samples[4], -32768);
    }

    #[test]
    fn test_f32_to_i16_clamping() {
        let f32_samples = vec![2.0, -2.0, 1.5, -1.5];
        let i16_samples = f32_to_i16(&f32_samples);

        assert_eq!(i16_samples[0], 32767); // clamped to 1.0
        assert_eq!(i16_samples[1], -32768); // clamped to -1.0
        assert_eq!(i16_samples[2], 32767);
        assert_eq!(i16_samples[3], -32768);
    }

    #[test]
    fn test_i32_to_f32_conversion() {
        let i32_samples = vec![0, 1073741824, 2147483647, -1073741824, -2147483648];
        let f32_samples = i32_to_f32(&i32_samples);

        assert_eq!(f32_samples.len(), 5);
        assert_eq!(f32_samples[0], 0.0);
        assert!((f32_samples[1] - 0.5).abs() < 0.01);
        assert!((f32_samples[2] - 1.0).abs() < 0.0001);
        assert!((f32_samples[3] + 0.5).abs() < 0.01);
        assert!((f32_samples[4] + 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_f32_to_i32_conversion() {
        let f32_samples = vec![0.0, 0.5, 1.0, -0.5, -1.0];
        let i32_samples = f32_to_i32(&f32_samples);

        assert_eq!(i32_samples.len(), 5);
        assert_eq!(i32_samples[0], 0);
        assert!((i32_samples[1] - 1073741823).abs() <= 1);
        assert_eq!(i32_samples[2], 2147483647);
        assert!((i32_samples[3] + 1073741823).abs() <= 1);
        assert_eq!(i32_samples[4], -2147483648);
    }

    #[test]
    fn test_transmute_f32_to_u8() {
        let samples = vec![0.0_f32, 1.0, -1.0];
        let bytes = transmute_f32_to_u8(&samples);

        assert_eq!(bytes.len(), 12); // 3 floats * 4 bytes
    }

    #[test]
    fn test_transmute_u8_to_f32_aligned() {
        // Create properly aligned f32 data
        let samples = vec![0.0_f32, 1.0, 2.0];
        let bytes = transmute_f32_to_u8(&samples);

        // Should succeed with proper alignment
        let result = transmute_u8_to_f32(bytes);
        assert!(result.is_some());

        let f32_slice = result.unwrap();
        assert_eq!(f32_slice.len(), 3);
        assert_eq!(f32_slice[0], 0.0);
        assert_eq!(f32_slice[1], 1.0);
        assert_eq!(f32_slice[2], 2.0);
    }

    #[test]
    fn test_transmute_u8_to_f32_wrong_length() {
        let bytes = vec![0, 0, 0]; // Not a multiple of 4
        let result = transmute_u8_to_f32(&bytes);

        assert!(result.is_none());
    }

    #[test]
    fn test_round_trip_i16() {
        let original = vec![0, 8192, 16384, -8192, -16384];
        let f32_samples = i16_to_f32(&original);
        let round_trip = f32_to_i16(&f32_samples);

        for (orig, rt) in original.iter().zip(round_trip.iter()) {
            assert!((orig - rt).abs() <= 1); // Allow 1 LSB error from rounding
        }
    }

    #[test]
    fn test_round_trip_i32() {
        let original = vec![0, 536870912, 1073741824, -536870912, -1073741824];
        let f32_samples = i32_to_f32(&original);
        let round_trip = f32_to_i32(&f32_samples);

        for (orig, rt) in original.iter().zip(round_trip.iter()) {
            // Allow larger error due to f32 precision limits
            let diff = (orig - rt).abs();
            assert!(diff < 1000, "Difference too large: {}", diff);
        }
    }
}
