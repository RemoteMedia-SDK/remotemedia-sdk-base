//! Reusable `Vec<f32>` pool for real-time audio paths.
//!
//! In the steady state of a streaming audio pipeline, every node
//! allocates and drops `Vec<f32>` sample buffers once per frame. At
//! 48 kHz / 20 ms / 3 clones per frame this is ~2.88 MB/s of
//! alloc/free traffic — small per-op, but enough to drive allocator
//! fragmentation and periodic GC-like pauses at the tail (p99.9+).
//!
//! [`AudioBufferPool`] lets a node "rent" a zeroed-capacity `Vec<f32>`,
//! fill and ship it downstream via [`PooledAudioBuf`], and return it
//! to the pool on drop. The pool is backed by [`crossbeam::queue::ArrayQueue`]
//! so get/return are wait-free lock-free and safe to call from any thread
//! (tokio worker, dedicated RT thread, etc.).
//!
//! # Design notes
//!
//! - **Not a fixed-size pool.** The pool has a bounded queue depth; if
//!   the queue is full on return, the buffer is dropped. This bounds
//!   memory while keeping steady-state operations fast.
//! - **Capacity, not length.** Returned buffers keep their allocated
//!   capacity but are cleared (`len = 0`). Nodes extend them via
//!   `push` / `extend_from_slice` / `resize`.
//! - **Not zero-copy to the downstream protobuf / IPC boundary.** That
//!   conversion still happens at the transport edge. The pool's
//!   job is to kill the per-frame allocation between nodes.
//! - **Not part of `RuntimeData` yet.** `RuntimeData::Audio` still owns
//!   a raw `Vec<f32>`. Wiring a pooled buffer through the enum requires
//!   a follow-up; for now nodes can use the pool internally for scratch
//!   buffers and return a raw `Vec<f32>` at the boundary.
//!
//! # Example
//!
//! ```
//! use remotemedia_core::data::audio_buffer_pool::AudioBufferPool;
//! use std::sync::Arc;
//!
//! let pool = Arc::new(AudioBufferPool::new(16, 960));
//! {
//!     let mut buf = pool.acquire();
//!     buf.extend_from_slice(&[0.0; 960]);
//!     // ... hand buf downstream; when dropped, buffer returns to pool.
//! }
//! assert_eq!(pool.len(), 1);
//! ```

use crossbeam::queue::ArrayQueue;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// A bounded lock-free pool of reusable `Vec<f32>` buffers.
///
/// Cheap to clone via `Arc`; all operations are non-blocking.
#[derive(Debug)]
pub struct AudioBufferPool {
    queue: ArrayQueue<Vec<f32>>,
    /// Minimum capacity that freshly-allocated (pool-miss) buffers use.
    min_capacity: usize,
}

impl AudioBufferPool {
    /// Create a new pool.
    ///
    /// * `queue_depth` — maximum number of buffers held in the pool.
    ///   Returns beyond this drop the buffer normally.
    /// * `min_capacity` — capacity for freshly-allocated buffers when
    ///   the pool is empty (pool miss). Choose ≥ your typical audio
    ///   frame sample count, e.g. `960` for 48 kHz / 20 ms / mono.
    pub fn new(queue_depth: usize, min_capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(queue_depth.max(1)),
            min_capacity,
        }
    }

    /// Acquire a cleared buffer.
    ///
    /// Fast path (pool hit): pops an existing buffer, clears length to 0,
    /// capacity preserved. Slow path (pool miss): allocates a new
    /// `Vec<f32>` with `min_capacity` headroom.
    pub fn acquire(self: &Arc<Self>) -> PooledAudioBuf {
        let inner = match self.queue.pop() {
            Some(mut v) => {
                v.clear();
                v
            }
            None => Vec::with_capacity(self.min_capacity),
        };
        PooledAudioBuf {
            inner: Some(inner),
            pool: Some(Arc::clone(self)),
        }
    }

    /// Current number of buffers in the pool.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Push a buffer back into the pool (internal).
    fn release(&self, mut buf: Vec<f32>) {
        // Drop references to samples but keep capacity.
        buf.clear();
        // If full, ArrayQueue::push returns Err with the buffer; just drop it.
        let _ = self.queue.push(buf);
    }
}

/// RAII wrapper over a `Vec<f32>` checked out of an [`AudioBufferPool`].
///
/// Derefs to `Vec<f32>`; returns itself to the pool on drop. Use
/// [`PooledAudioBuf::into_inner`] to detach the buffer (no pool return)
/// when you need to hand ownership to a sink (e.g. into `RuntimeData::Audio`).
#[derive(Debug)]
pub struct PooledAudioBuf {
    inner: Option<Vec<f32>>,
    pool: Option<Arc<AudioBufferPool>>,
}

impl PooledAudioBuf {
    /// Detach the inner `Vec<f32>`; the pool will not recover this buffer.
    pub fn into_inner(mut self) -> Vec<f32> {
        self.pool.take();
        self.inner.take().expect("inner taken twice")
    }
}

impl Deref for PooledAudioBuf {
    type Target = Vec<f32>;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("buffer taken")
    }
}

impl DerefMut for PooledAudioBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("buffer taken")
    }
}

impl Drop for PooledAudioBuf {
    fn drop(&mut self) {
        if let (Some(buf), Some(pool)) = (self.inner.take(), self.pool.take()) {
            pool.release(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_empty_buffer_with_min_capacity() {
        let pool = Arc::new(AudioBufferPool::new(4, 960));
        let buf = pool.acquire();
        assert_eq!(buf.len(), 0);
        assert!(buf.capacity() >= 960);
    }

    #[test]
    fn drop_returns_buffer_to_pool() {
        let pool = Arc::new(AudioBufferPool::new(4, 960));
        assert!(pool.is_empty());
        {
            let mut b = pool.acquire();
            b.extend_from_slice(&[0.1; 480]);
        }
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn pool_reuses_capacity_not_contents() {
        let pool = Arc::new(AudioBufferPool::new(4, 960));
        let original_ptr = {
            let mut b = pool.acquire();
            b.extend_from_slice(&[0.5; 960]);
            b.as_ptr() as usize
        };
        // Buffer returned on drop. Re-acquire — should reuse same allocation.
        let b2 = pool.acquire();
        assert_eq!(b2.len(), 0, "re-acquired buffer should be cleared");
        assert!(b2.capacity() >= 960);
        // Pointer equality is allocator-dependent but should hold for
        // single-threaded tests.
        assert_eq!(b2.as_ptr() as usize, original_ptr);
    }

    #[test]
    fn pool_drops_excess_buffers_beyond_queue_depth() {
        let pool = Arc::new(AudioBufferPool::new(2, 16));
        // Acquire 3, drop all → pool caps at depth 2.
        {
            let _a = pool.acquire();
            let _b = pool.acquire();
            let _c = pool.acquire();
        }
        assert!(pool.len() <= 2);
    }

    #[test]
    fn into_inner_detaches_buffer() {
        let pool = Arc::new(AudioBufferPool::new(4, 960));
        let buf = pool.acquire();
        let v = buf.into_inner();
        assert_eq!(v.len(), 0);
        // Pool should not receive the buffer back.
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn concurrent_acquire_release_is_safe() {
        use std::thread;

        let pool = Arc::new(AudioBufferPool::new(32, 256));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let mut b = pool.acquire();
                        b.extend_from_slice(&[0.0; 256]);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Pool should have buffers (<= queue depth).
        assert!(pool.len() <= 32);
    }
}
