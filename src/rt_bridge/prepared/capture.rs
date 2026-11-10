//! One explicitly authorized copy from device-owned input into prepared storage.

use super::{publish_retained, PreparedAudioError, PreparedAudioProducer};
use std::sync::atomic::Ordering;

/// Outcome of a callback's borrowed input adaptation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureProgress {
    /// Prefix copied into prepared storage, in interleaved samples.
    pub accepted_samples: usize,
    /// Why the remaining suffix was not accepted. The caller must explicitly
    /// handle or report that suffix; device-owned input cannot be retained here.
    pub error: Option<PreparedAudioError>,
}

/// Accumulates variable device callback sizes directly into prepared packets.
///
/// Each accepted sample is copied exactly once from the borrowed device buffer.
/// This compatibility boundary is rejected under strict zero-copy policy before
/// allocation or device startup. Callback operations perform no allocation,
/// release, ledger calls, locking or waiting, subject to the selected allocator's
/// mutable slice contract. Destroy this handle off the callback thread after
/// stopping the device. Drop publishes any partial final packet using a reserved
/// queue slot; explicit `finish` also reports premature worker shutdown.
pub struct PreparedCaptureProducer {
    pub(crate) producer: PreparedAudioProducer,
    pub(crate) channels: u16,
    pub(crate) sample_rate: u32,
    pub(crate) finished: bool,
}

impl PreparedCaptureProducer {
    /// Copy a borrowed callback into the chosen output storage. Packet timestamps
    /// identify their first sample; an existing partial packet keeps its timestamp.
    /// Overflow of timestamp arithmetic produces an absent timestamp.
    pub fn try_capture(&mut self, input: &[f32], timestamp_us: Option<u64>) -> CaptureProgress {
        let mut progress = CaptureProgress {
            accepted_samples: 0,
            error: None,
        };
        if input.len() % usize::from(self.channels) != 0 {
            progress.error = Some(PreparedAudioError::InvalidLayout);
            return progress;
        }
        while progress.accepted_samples < input.len() {
            if let Err(error) =
                self.capture_part(input, timestamp_us, &mut progress.accepted_samples)
            {
                progress.error = Some(error);
                break;
            }
        }
        progress
    }

    fn capture_part(
        &mut self,
        input: &[f32],
        timestamp_us: Option<u64>,
        accepted: &mut usize,
    ) -> Result<(), PreparedAudioError> {
        if self.finished
            || self.producer.stopped.load(Ordering::Acquire)
            || self.producer.input.is_abandoned()
        {
            return Err(PreparedAudioError::Stopped);
        }
        // The last slot belongs to finish/Drop, never to ordinary publication.
        if self.producer.input.slots() <= 1 {
            return Err(PreparedAudioError::Full);
        }
        if self.producer.current.is_none() {
            let mut frame = self
                .producer
                .ready
                .pop()
                .map_err(|_| PreparedAudioError::Unavailable)?;
            let offset = (*accepted / usize::from(self.channels)) as u64;
            frame.timestamp_us = timestamp_us.and_then(|time| {
                offset
                    .checked_mul(1_000_000)
                    .and_then(|offset| time.checked_add(offset / u64::from(self.sample_rate)))
            });
            self.producer.current = Some(frame);
        }
        let frame = self
            .producer
            .current
            .as_mut()
            .expect("prepared capture loan");
        let count = (input.len() - *accepted).min(frame.loan.len() - frame.filled);
        frame.loan.as_mut_slice()[frame.filled..frame.filled + count]
            .copy_from_slice(&input[*accepted..*accepted + count]);
        frame.filled += count;
        frame
            .capture
            .as_mut()
            .expect("capture accounting")
            .copied_bytes += (count as u64) * 4;
        *accepted += count;
        if frame.filled == frame.loan.len() {
            publish_retained(&mut self.producer.current, &mut self.producer.input)?;
        }
        Ok(())
    }

    /// After stopping callbacks, publish a partial final packet and close input.
    /// This only enqueues: keep consuming output before shutting down the bridge.
    /// Processing failures and output overflow remain visible in bridge stats.
    /// Idempotent after success. A stopped worker leaves the partial owner here
    /// for release/accounting on this control thread.
    pub fn finish(&mut self) -> Result<(), PreparedAudioError> {
        if self.finished {
            return Ok(());
        }
        if self.producer.stopped.load(Ordering::Acquire) || self.producer.input.is_abandoned() {
            return Err(PreparedAudioError::Stopped);
        }
        publish_retained(&mut self.producer.current, &mut self.producer.input)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for PreparedCaptureProducer {
    fn drop(&mut self) {
        // Control-thread destruction only. A failed finish retains the frame for
        // ordinary field destruction, which still reports the exact copied bytes.
        let _ = self.finish();
    }
}
