//! Shared codec-frame assembly with transport-specific error conversion.
pub(super) use remotemedia_core::audio::pcm_frames::AudioPcmPacketizer;
use remotemedia_core::audio::pcm_frames::PcmFrameError;

impl From<PcmFrameError> for crate::Error {
    fn from(error: PcmFrameError) -> Self {
        match error {
            PcmFrameError::LengthOverflow => Self::InvalidData(error.to_string()),
            PcmFrameError::CopyRejected(error) => Self::EncodingError(error.to_string()),
        }
    }
}

#[cfg(test)]
use remotemedia_types::{CopyContext, CopyReason, SharedBuffer};

#[cfg(test)]
pub(super) trait PcmFixtureExt {
    fn push(&mut self, samples: &[f32]) -> Vec<(SharedBuffer<f32>, usize)>;
}

#[cfg(test)]
impl PcmFixtureExt for AudioPcmPacketizer {
    fn push(&mut self, samples: &[f32]) -> Vec<(SharedBuffer<f32>, usize)> {
        // Borrowed test fixtures create their input owner outside the measured path.
        self.append(samples.to_vec().into(), CopyContext::legacy())
            .unwrap();
        std::iter::from_fn(|| {
            self.next_frame().unwrap().map(|pcm| {
                let len = pcm.len();
                (pcm, len)
            })
        })
        .collect()
    }
}

#[cfg(test)]
#[path = "audio_packetizer_tests.rs"]
mod tests;
