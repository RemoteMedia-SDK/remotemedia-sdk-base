//! Device callback ownership and control-thread draining.

use crate::rt_bridge::{
    PreparedAudioConfig, PreparedAudioConsumer, PreparedAudioError, PreparedCaptureProducer,
    RtBridge, RtBridgeConfig,
};
use crate::{
    data::RuntimeData,
    nodes::{NodeRuntimeContext, SyncStreamingNode},
};
use anyhow::{anyhow, Result};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;

#[derive(Default)]
pub(super) struct CaptureCounters {
    pub dropped_samples: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub device_error: AtomicBool,
}

/// CPAL may destroy this on its audio thread. Return the entire producer through
/// an empty one-slot queue; the control thread keeps the receiver alive through
/// stream destruction and handback. No buffer/ledger destruction runs here.
pub(super) struct CaptureCallback {
    producer: Option<PreparedCaptureProducer>,
    returned: rtrb::Producer<PreparedCaptureProducer>,
    counters: Arc<CaptureCounters>,
    limit: Option<CaptureLimit>,
}

struct CaptureLimit {
    remaining: usize,
    complete: Arc<AtomicBool>,
}

impl CaptureCallback {
    pub fn capture(&mut self, samples: &[f32]) {
        // Samples beyond a finite recording's requested interval are not part
        // of that recording. Do not copy or report them as queue losses.
        let requested = self
            .limit
            .as_ref()
            .map_or(samples.len(), |limit| samples.len().min(limit.remaining));
        let progress = self
            .producer
            .as_mut()
            .expect("live callback")
            .try_capture(&samples[..requested], None);
        let rejected = requested - progress.accepted_samples;
        self.counters
            .dropped_samples
            .fetch_add(rejected as u64, Ordering::Relaxed);
        if let Some(limit) = &mut self.limit {
            limit.remaining -= progress.accepted_samples;
            if limit.remaining == 0 {
                limit.complete.store(true, Ordering::Release);
            }
        }
    }
}

impl Drop for CaptureCallback {
    fn drop(&mut self) {
        // Exactly one push, into a dedicated capacity-one queue. Its receiver is
        // held until this callback is destroyed. This cannot run out of slots.
        let _ = self
            .returned
            .push(self.producer.take().expect("single callback return"));
    }
}

struct CaptureEcho;
impl SyncStreamingNode for CaptureEcho {
    fn node_type(&self) -> &str {
        "CaptureEcho"
    }
    fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        Ok(data)
    }
}

pub(super) struct CaptureWorker {
    bridge: Option<RtBridge>,
    consumer: Option<PreparedAudioConsumer>,
    returned: rtrb::Consumer<PreparedCaptureProducer>,
    pub counters: Arc<CaptureCounters>,
}

impl CaptureWorker {
    pub fn prepare(
        context: NodeRuntimeContext,
        audio: PreparedAudioConfig,
        complete: Option<Arc<AtomicBool>>,
    ) -> Result<(Self, CaptureCallback)> {
        let (bridge, producer, consumer) = RtBridge::spawn_prepared_capture(
            CaptureEcho,
            RtBridgeConfig::default(),
            audio,
            context,
        )?;
        let (returned_tx, returned) = rtrb::RingBuffer::new(1);
        let counters = Arc::new(CaptureCounters::default());
        let callback = CaptureCallback {
            producer: Some(producer),
            returned: returned_tx,
            counters: counters.clone(),
            limit: complete.map(|complete| CaptureLimit {
                remaining: audio.samples,
                complete,
            }),
        };
        Ok((
            Self {
                bridge: Some(bridge),
                consumer: Some(consumer),
                returned,
                counters,
            },
            callback,
        ))
    }

    pub fn pump(&mut self, output: &mpsc::Sender<RuntimeData>) -> Result<()> {
        for _ in 0..64 {
            match self
                .consumer
                .as_mut()
                .expect("live consumer")
                .try_read(Clone::clone)
            {
                Ok(Some(packet)) => self.send(output, packet),
                Ok(None) | Err(PreparedAudioError::Full) => break,
                Err(error) => return Err(anyhow!("capture worker stopped: {error:?}")),
            }
        }
        self.check_losses(self.bridge.as_ref().expect("live bridge").stats())
    }

    fn send(&self, output: &mpsc::Sender<RuntimeData>, packet: RuntimeData) {
        if output.try_send(packet).is_err() {
            self.counters
                .dropped_packets
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn check_losses(&self, stats: crate::rt_bridge::RtBridgeStats) -> Result<()> {
        let samples = self.counters.dropped_samples.load(Ordering::Relaxed);
        let packets = self.counters.dropped_packets.load(Ordering::Relaxed);
        let device_error = self.counters.device_error.load(Ordering::Acquire);
        if device_error
            || samples != 0
            || packets != 0
            || stats.process_errors != 0
            || stats.output_overflows != 0
        {
            anyhow::bail!("capture failed: device_error={device_error}, dropped_samples={samples}, output_packets={packets}, process_errors={}, bridge_overflows={}", stats.process_errors, stats.output_overflows);
        }
        Ok(())
    }

    /// Call only after destroying the device stream. Unlike assuming stream Drop
    /// joined, explicit handback also covers WASAPI's early-disconnect path.
    fn reclaim(&mut self) -> Result<()> {
        let mut producer = loop {
            if let Ok(producer) = self.returned.pop() {
                break producer;
            }
            if self.returned.is_abandoned() {
                // Re-read after acquiring the final producer publication.
                break self
                    .returned
                    .pop()
                    .map_err(|_| anyhow!("capture callback did not return its producer"))?;
            }
            std::thread::yield_now();
        };
        producer
            .finish()
            .map_err(|error| anyhow!("capture final publication: {error:?}"))
    }

    pub fn finish(mut self, output: &mpsc::Sender<RuntimeData>) -> Result<()> {
        let reclaim = self.reclaim();
        let joined = self
            .bridge
            .take()
            .expect("live bridge")
            .shutdown_checked()
            .map_err(|_| anyhow!("capture worker panicked"));
        for packet in self
            .consumer
            .take()
            .expect("live consumer")
            .drain_after_shutdown()
        {
            self.send(output, packet);
        }
        reclaim?;
        self.check_losses(joined?)
    }
}

#[cfg(test)]
mod tests;
