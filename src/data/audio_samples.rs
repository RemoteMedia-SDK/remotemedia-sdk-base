//! Audio storage and the pool-to-shared-owner bridge.
//!
//! Moving a pool loan into SharedBuffer preserves its allocation and returns it
//! to the pool only after the final clone or slice releases its owner.

pub use remotemedia_types::AudioSamples;

use super::audio_buffer_pool::PooledAudioBuf;
use remotemedia_types::SharedBuffer;

impl From<PooledAudioBuf> for AudioSamples {
    fn from(buffer: PooledAudioBuf) -> Self {
        SharedBuffer::from_owner(buffer).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_buffer_pool::AudioBufferPool;
    use std::sync::Arc;

    #[test]
    fn prepared_pool_output_retains_allocation_until_last_reader() {
        let pool = Arc::new(AudioBufferPool::new(2, 256));
        let mut buffer = pool.acquire();
        buffer.resize(128, 0.0);
        let pointer = buffer.as_ptr();
        assert_eq!(pointer as usize % std::mem::align_of::<f32>(), 0);
        let mut output = remotemedia_types::BufferLoan::from_owner(buffer);
        output.fill(0.25);
        let published = output.freeze();
        assert_eq!(published.as_ptr(), pointer);
        let retained = published.slice(4..8).unwrap();
        drop(published);
        assert!(pool.is_empty());
        assert_eq!(retained.as_slice(), &[0.25; 4]);
        std::thread::spawn(move || drop(retained)).join().unwrap();
        assert_eq!(pool.acquire().as_ptr(), pointer);
    }

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
    fn pooled_storage_returns_only_after_last_slice_releases() {
        let pool = Arc::new(AudioBufferPool::new(4, 256));
        let mut loan = pool.acquire();
        loan.extend_from_slice(&[0.5; 128]);
        let pointer = loan.as_ptr();
        let capacity = loan.capacity();
        let samples: AudioSamples = loan.into();
        assert_eq!(samples.as_ptr(), pointer);
        let clone = samples.clone();
        assert_eq!(clone.as_ptr(), pointer);
        let slice = samples.into_shared().slice(1..127).unwrap();
        assert!(slice.slice(usize::MAX..usize::MAX).is_err());
        drop(clone);
        assert!(pool.is_empty());
        assert_eq!(slice.as_slice(), &[0.5; 126]);
        std::thread::spawn(move || drop(slice)).join().unwrap();
        assert_eq!(pool.len(), 1);
        let reused = pool.acquire();
        assert_eq!(reused.as_ptr(), pointer);
        assert_eq!(reused.capacity(), capacity);
        assert!(reused.is_empty());
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
    fn into_arc_preserves_values() {
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
        assert_eq!(back.variant_name(), "Shared");
        assert_eq!(back.as_slice(), &[1.5, 2.5, 3.5]);
    }
}
