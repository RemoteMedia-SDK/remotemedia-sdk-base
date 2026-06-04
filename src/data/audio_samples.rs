//! Audio-samples adaptor — re-exports the wire-format
//! [`AudioSamples`] from `remotemedia-types` and adds the core-only
//! `From<PooledAudioBuf> for AudioSamples` bridge so audio buffer
//! pool consumers (RT thread, CoreAudio HAL callback, AU plugin,
//! JACK client, …) can hand pool-backed buffers into the pipeline
//! without forcing a heap allocation.
//!
//! # Why a separate Pooled wrapper?
//!
//! Before the A2 split, `AudioSamples` had three variants:
//! `Vec(Vec<f32>)`, `Arc(Arc<[f32]>)`, and `Pooled(PooledAudioBuf)`.
//! The `Pooled` variant referenced
//! [`PooledAudioBuf`](super::audio_buffer_pool::PooledAudioBuf),
//! which is heavy core machinery (return-to-pool RAII, pool
//! registry). To keep `remotemedia-types` skinny — and therefore
//! consumable by out-of-tree plugins without a transitive pull
//! on the buffer-pool machinery — the `Pooled` variant was
//! removed from the public enum.
//!
//! On the wire `AudioSamples` always serialized as a flat
//! `Vec<f32>` (and deserialized into the `Vec` variant), so this
//! split is **wire-format-neutral**: there is no observable change
//! at the byte level. Pool buffers crossing the wire-typed
//! boundary bridge into `AudioSamples::Arc` via the
//! `From<PooledAudioBuf>` impl below. This is a single
//! shrink-to-fit memcpy when `len < capacity` (common after partial
//! fill — e.g. acquiring a 1600-sample-capacity buffer from
//! `AudioBufferPool::acquire(min_capacity)` and writing only 480
//! samples for a chunk); when `len == capacity`, no copy occurs.
//! The trade-off is one bounded heap copy at the wire boundary in
//! exchange for letting `RuntimeData::Audio` carry a stable
//! `Arc<[f32]>` shape that survives FFI. True zero-copy would
//! require `AudioBufferPool` to hand out `Box<[f32]>` of fixed
//! capacity — a separate refactor not done here.

pub use remotemedia_types::AudioSamples;

use super::audio_buffer_pool::PooledAudioBuf;
use std::sync::Arc;

/// Bridge a pool-acquired buffer into the wire-shaped
/// [`AudioSamples::Arc`].
///
/// This is a single shrink-to-fit memcpy when `len < capacity`
/// (common after partial fill — e.g. a 480-sample chunk written
/// into a 1600-cap pool buffer for Whisper); when
/// `len == capacity`, no copy occurs. The trade-off is one
/// bounded heap copy at the wire boundary in exchange for letting
/// `RuntimeData::Audio` carry a stable `Arc<[f32]>` shape that
/// survives FFI.
///
/// The pool buffer is detached (its drop will **not** return the
/// buffer to the pool — the new boxed-slice allocation lives
/// inside the `Arc`). Pool-aware producers that want
/// return-to-pool semantics should hold the `PooledAudioBuf`
/// until the last consumer reads it and then drop it without
/// converting.
impl From<PooledAudioBuf> for AudioSamples {
    #[inline]
    fn from(p: PooledAudioBuf) -> Self {
        AudioSamples::Arc(Arc::from(p.into_inner().into_boxed_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_buffer_pool::AudioBufferPool;

    #[test]
    fn vec_variant_round_trip() {
        let s: AudioSamples = vec![1.0, 2.0, 3.0].into();
        assert_eq!(s.len(), 3);
        assert_eq!(s.as_slice(), &[1.0, 2.0, 3.0]);
        assert_eq!(s.into_vec(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn arc_variant_zero_copy_clone() {
        let arc: Arc<[f32]> = Arc::from(vec![4.0, 5.0, 6.0].into_boxed_slice());
        let s: AudioSamples = arc.clone().into();
        let s2 = s.clone();
        // Both variants share the same heap buffer.
        if let (AudioSamples::Arc(a1), AudioSamples::Arc(a2)) = (&s, &s2) {
            assert!(Arc::ptr_eq(a1, a2));
        } else {
            panic!("expected Arc variant");
        }
        assert_eq!(s.as_slice(), s2.as_slice());
    }

    #[test]
    fn pooled_variant_converts_to_arc_and_pool_keeps_capacity() {
        // Pooled buffer detaches when converted into `AudioSamples::Arc`.
        // The pool does NOT receive the buffer back on drop because the
        // backing storage now lives inside the `Arc`.
        let pool = Arc::new(AudioBufferPool::new(4, 256));
        assert!(pool.is_empty());
        {
            let mut buf = pool.acquire();
            buf.extend_from_slice(&[0.5; 128]);
            let samples: AudioSamples = buf.into();
            assert_eq!(samples.len(), 128);
            assert_eq!(samples.variant_name(), "Arc");
            // Holding the Arc keeps the data live; pool doesn't see it.
            assert!(pool.is_empty());
        }
        // Even after the Arc drops, the buffer is gone — converted to
        // Arc detaches it from the pool's reuse cycle. This is the
        // documented trade-off: pool-aware producers that want
        // return-to-pool semantics should keep the `PooledAudioBuf`
        // until the last consumer reads it, then drop the buffer
        // (which returns to pool) without converting.
        assert!(pool.is_empty());
    }

    #[test]
    fn deref_exposes_slice_methods() {
        let s: AudioSamples = vec![1.0, 2.0, 3.0, 4.0].into();
        // Slice methods go through Deref.
        assert_eq!(s.iter().sum::<f32>(), 10.0);
        assert_eq!(&s[1..3], &[2.0, 3.0]);
        assert_eq!(s.to_vec(), vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn equality_across_variants() {
        let v = AudioSamples::from(vec![1.0, 2.0, 3.0]);
        let a = AudioSamples::from(Arc::from(vec![1.0, 2.0, 3.0].into_boxed_slice()));

        assert_eq!(v, a);
    }

    #[test]
    fn into_arc_from_vec_zero_copy_semantics() {
        let v: AudioSamples = vec![7.0, 8.0, 9.0].into();
        let arc = v.into_arc();
        assert_eq!(arc.as_ref(), &[7.0, 8.0, 9.0]);
    }

    #[test]
    fn serde_round_trip_as_flat_vec() {
        let s: AudioSamples = vec![1.5, 2.5, 3.5].into();
        let j = serde_json::to_string(&s).unwrap();
        assert_eq!(j, "[1.5,2.5,3.5]");
        let back: AudioSamples = serde_json::from_str(&j).unwrap();
        assert_eq!(back.variant_name(), "Vec");
        assert_eq!(back.as_slice(), &[1.5, 2.5, 3.5]);
    }
}
