use remotemedia_types::AudioSamples;
use rubato::audioadapter::Adapter;
use std::collections::VecDeque;

/// Retains immutable publications across arbitrary packet boundaries. Rubato
/// reads these directly into its filter history, without a framework gather copy.
pub(super) struct PendingInput {
    chunks: VecDeque<AudioSamples>,
    offset: usize,
    frames: usize,
    channels: usize,
}
impl PendingInput {
    pub(super) fn new(channels: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            offset: 0,
            frames: 0,
            channels,
        }
    }
    pub(super) fn frames(&self) -> usize {
        self.frames
    }
    pub(super) fn push(&mut self, samples: AudioSamples) {
        self.frames += samples.len() / self.channels;
        if !samples.is_empty() {
            self.chunks.push_back(samples);
        }
    }
    pub(super) fn consume(&mut self, frames: usize) {
        assert!(frames <= self.frames);
        self.frames -= frames;
        let mut samples = frames * self.channels;
        while samples > 0 {
            let available = self.chunks.front().unwrap().len() - self.offset;
            let count = samples.min(available);
            samples -= count;
            self.offset += count;
            if count == available {
                self.chunks.pop_front();
                self.offset = 0;
            }
        }
    }
    pub(super) fn clear(&mut self) {
        self.chunks.clear();
        self.offset = 0;
        self.frames = 0;
    }
}
impl<'a> Adapter<'a, f32> for PendingInput {
    unsafe fn read_sample_unchecked(&self, channel: usize, frame: usize) -> f32 {
        let mut index = frame * self.channels + channel + self.offset;
        for chunk in &self.chunks {
            if index < chunk.len() {
                return chunk[index];
            }
            index -= chunk.len();
        }
        unreachable!("adapter caller validated channel and frame bounds")
    }
    fn channels(&self) -> usize {
        self.channels
    }
    fn frames(&self) -> usize {
        self.frames
    }

    // Rubato uses this method to populate its required filter history. Traverse
    // chunks once per channel instead of repeatedly searching for each sample.
    fn copy_from_channel_to_slice(&self, channel: usize, skip: usize, target: &mut [f32]) -> usize {
        if channel >= self.channels || skip >= self.frames {
            return 0;
        }
        let count = target.len().min(self.frames - skip);
        let mut index = skip * self.channels + self.offset;
        let mut written = 0;
        for chunk in &self.chunks {
            if index >= chunk.len() {
                index -= chunk.len();
                continue;
            }
            for frame in chunk[index..].chunks_exact(self.channels) {
                if written == count {
                    return written;
                }
                target[written] = frame[channel];
                written += 1;
            }
            index = 0;
        }
        written
    }
}
