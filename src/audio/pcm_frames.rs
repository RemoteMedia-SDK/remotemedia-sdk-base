//! Retain input owners until a codec frame can borrow one contiguous range.
/// Frame assembly failure. Rejection preserves the pending owners for retry or clearing.
#[derive(Debug, thiserror::Error)]
pub enum PcmFrameError {
    #[error("PCM queue length overflow")]
    LengthOverflow,
    #[error(transparent)]
    CopyRejected(#[from] remotemedia_types::CopyRejected),
}

type Result<T> = std::result::Result<T, PcmFrameError>;
use remotemedia_types::{CopyContext, CopyReason, SharedBuffer};
use std::collections::VecDeque;

#[derive(Debug)]
struct PendingPcm {
    samples: SharedBuffer<f32>,
    offset: usize,
    copies: CopyContext,
}

/// Owning PCM queue for fixed-size codec frames.
#[derive(Debug)]
pub struct AudioPcmPacketizer {
    frame_size: usize,
    pending: VecDeque<PendingPcm>,
    remaining: usize,
}

impl AudioPcmPacketizer {
    /// Create a queue; panics if frame_size is zero.
    pub fn new(frame_size: usize) -> Self {
        assert!(frame_size > 0);
        Self {
            frame_size,
            pending: VecDeque::new(),
            remaining: 0,
        }
    }

    pub fn append(&mut self, samples: SharedBuffer<f32>, copies: &CopyContext) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        self.remaining = self
            .remaining
            .checked_add(samples.len())
            .ok_or(PcmFrameError::LengthOverflow)?;
        if let Some(last) = self.pending.back_mut() {
            if last.copies.same_authority(copies) {
                if let Some(joined) = last.samples.try_join(&samples) {
                    last.samples = joined;
                    return Ok(());
                }
            }
        }
        self.pending.push_back(PendingPcm {
            samples,
            offset: 0,
            copies: copies.clone(),
        });
        Ok(())
    }

    pub fn next_frame(&mut self) -> Result<Option<SharedBuffer<f32>>> {
        if self.remaining < self.frame_size {
            return Ok(None);
        }
        let first = self.pending.front_mut().expect("nonempty PCM queue");
        if first.samples.len() - first.offset >= self.frame_size {
            let frame = first
                .samples
                .slice(first.offset..first.offset + self.frame_size)
                .expect("frame bounded by first owner");
            self.consume(self.frame_size);
            return Ok(Some(frame));
        }
        self.assemble(self.frame_size).map(Some)
    }

    pub fn finish(&mut self) -> Result<Option<(SharedBuffer<f32>, usize)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.remaining >= self.frame_size {
            return self
                .next_frame()
                .map(|frame| frame.map(|pcm| (pcm, self.frame_size)));
        }
        let real = self.remaining;
        self.assemble(real).map(|frame| Some((frame, real)))
    }

    fn assemble(&mut self, real: usize) -> Result<SharedBuffer<f32>> {
        // Authorize all contributing owners before destination allocation or reads.
        // A later permissive caller or EOS must not override a queued strict owner.
        let mut left = real;
        for part in &self.pending {
            let count = left.min(part.samples.len() - part.offset);
            part.copies
                .check(CopyReason::VecMaterialization, (count * 4) as u64)?;
            left -= count;
            if left == 0 {
                break;
            }
        }
        let mut output = vec![0.0; self.frame_size];
        let mut written = 0;
        while written < real {
            let part = self.pending.front().expect("authorized PCM owner");
            let count = (real - written).min(part.samples.len() - part.offset);
            part.copies
                .materialize(CopyReason::VecMaterialization, (count * 4) as u64, || {
                    output[written..written + count]
                        .copy_from_slice(&part.samples[part.offset..part.offset + count]);
                })?;
            self.consume(count);
            written += count;
        }
        // Padding is generated silence, not a copy of source payload bytes.
        Ok(output.into())
    }

    fn consume(&mut self, count: usize) {
        let part = self.pending.front_mut().expect("consumed PCM owner");
        part.offset += count;
        self.remaining -= count;
        if part.offset == part.samples.len() {
            self.pending.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.pending.clear();
        self.remaining = 0;
    }

    /// Number of source samples still retained by this queue.
    pub fn queued_samples(&self) -> usize {
        self.remaining
    }
}
