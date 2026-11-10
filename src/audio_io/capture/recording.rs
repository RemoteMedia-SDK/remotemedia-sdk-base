//! Finite recording directly into one selected output loan.

use super::*;
use remotemedia_types::{BufferAllocationError, BufferLoan, OutputAllocator, SharedBuffer};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// The streaming worker may request replacements while the device fills its loan.
// A finite recording has exactly one output; do not allocate unused spare buffers.
// The loan is allocated up front by the selected provider; the worker's
// refill is the only caller of the mutex. Prepared real-time support is the
// selected provider's.
struct RecordingAllocator(
    Mutex<Option<BufferLoan<f32>>>,
    remotemedia_types::PreparedRealtimeSupport,
);

impl OutputAllocator for RecordingAllocator {
    fn allocate_bytes(&self, _len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        Err(BufferAllocationError::Unavailable)
    }

    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        let loan = self
            .0
            .lock()
            .map_err(|_| BufferAllocationError::Unavailable)?
            .take()
            .ok_or(BufferAllocationError::Exhausted)?;
        if loan.len() != samples {
            return Err(BufferAllocationError::InvalidLength);
        }
        Ok(loan)
    }

    fn prepared_realtime_support(&self) -> remotemedia_types::PreparedRealtimeSupport {
        self.1
    }
}

pub(super) fn prepare(context: NodeRuntimeContext, samples: usize) -> Result<NodeRuntimeContext> {
    let support = context.output_allocator.prepared_realtime_support();
    let loan = context.output_allocator.allocate_audio(samples)?;
    Ok(context.with_output_allocator(Arc::new(RecordingAllocator(
        Mutex::new(Some(loan)),
        support,
    ))))
}

/// Record complete interleaved frames into the selected provider's single loan.
/// CPAL's borrowed samples require one authorized copy. No aggregation buffer is
/// used. This synchronous call blocks, but is also callable inside a Tokio runtime.
/// Zero frames return an empty provider allocation without opening the device.
pub fn capture_sync_shared(
    config: CaptureConfig,
    duration_ms: u32,
    ctx: &dyn NodeRuntimeContextRead,
) -> Result<SharedBuffer<f32>> {
    let samples = config.samples_for_duration(duration_ms)?;
    let context = AudioCapture::capture_context(ctx)?;
    if samples == 0 {
        return Ok(context.output_allocator.allocate_audio(0)?.freeze());
    }
    let capture = AudioCapture::start_backend(
        config,
        CaptureMode::Recording(samples),
        context,
        Arc::new(AtomicBool::new(false)),
        AudioCapture::open_device,
    )?;
    collect(
        capture,
        samples,
        Duration::from_millis(u64::from(duration_ms) + 1000),
    )
}

/// Compatibility recording. Unique full heap storage is recovered as a Vec;
/// any required materialization is accounted explicitly by the buffer API.
pub fn capture_sync(config: CaptureConfig, duration_ms: u32) -> Result<Vec<f32>> {
    let context = AudioCapture::legacy_context();
    Ok(capture_sync_shared(config, duration_ms, &context)?
        .try_into_vec(&context.materialization)?)
}

fn collect(
    mut capture: AudioCapture,
    samples: usize,
    timeout: Duration,
) -> Result<SharedBuffer<f32>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(packet) = capture.try_recv_packet()? {
            // Join before returning ownership: final device errors must propagate,
            // and temporary bridge owners must be gone for unique Vec recovery.
            capture.stop()?;
            let RuntimeData::Audio {
                samples: recorded, ..
            } = packet
            else {
                anyhow::bail!("recording returned a non-audio packet");
            };
            anyhow::ensure!(
                recorded.len() == samples,
                "recording ended early: {} of {samples} samples",
                recorded.len()
            );
            anyhow::ensure!(
                capture.try_recv_packet()?.is_none(),
                "recording returned extra packets"
            );
            return Ok(recorded.into_shared());
        }
        anyhow::ensure!(capture.thread.is_some(), "recording ended without audio");
        anyhow::ensure!(Instant::now() < deadline, "recording timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests;
