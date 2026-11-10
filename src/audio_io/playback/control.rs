use super::*;
use crate::rt_bridge::{PlaybackConsumer, PlaybackQueue, PlaybackReclaimer};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc,
};

/// The receiver outlives stream destruction and explicit callback handback.
/// Even CPAL backends that destroy callbacks on their device thread return all
/// packet/ring ownership before any final release or ledger completion.
pub(super) struct PlaybackCallback {
    consumer: Option<PlaybackConsumer>,
    returned: rtrb::Producer<PlaybackConsumer>,
    pub error: Arc<AtomicBool>,
    /// One input frame, and the device's channel count, when they differ.
    /// Allocated before the stream starts, never on the callback.
    map: Option<(Vec<f32>, usize)>,
}
impl PlaybackCallback {
    /// Render `channels`-channel packets onto a `device_channels` device.
    /// Call before the stream starts.
    pub fn map_channels(&mut self, channels: u16, device_channels: u16) {
        self.map = (channels != device_channels).then(|| {
            (
                vec![0.0; usize::from(channels.max(1))],
                usize::from(device_channels.max(1)),
            )
        });
    }

    pub fn render(&mut self, output: &mut [f32]) {
        let consumer = self.consumer.as_mut().expect("live playback callback");
        let failed = match &mut self.map {
            None => consumer.render(output).is_err(),
            Some((frame, device_channels)) => {
                let mut failed = false;
                for out in output.chunks_mut(*device_channels) {
                    failed |= consumer.render(frame).is_err();
                    super::super::map_frame(frame, out);
                }
                failed
            }
        };
        if failed {
            self.error.store(true, Ordering::Release);
        }
    }
}
impl Drop for PlaybackCallback {
    fn drop(&mut self) {
        // A single push into an empty capacity-one queue whose receiver remains
        // alive in PlaybackWorker until it has recovered this entire consumer.
        let _ = self
            .returned
            .push(self.consumer.take().expect("single callback return"));
    }
}

pub(super) struct PlaybackWorker {
    queue: Arc<Mutex<PlaybackQueue>>,
    retired: PlaybackReclaimer,
    returned: rtrb::Consumer<PlaybackConsumer>,
    stop: Arc<AtomicBool>,
    error: Arc<AtomicBool>,
    recovered: bool,
}
impl PlaybackWorker {
    pub fn new(
        queue: Arc<Mutex<PlaybackQueue>>,
        consumer: PlaybackConsumer,
        retired: PlaybackReclaimer,
        stop: Arc<AtomicBool>,
        error: Arc<AtomicBool>,
    ) -> (Self, PlaybackCallback) {
        let (returned_tx, returned) = rtrb::RingBuffer::new(1);
        let callback = PlaybackCallback {
            consumer: Some(consumer),
            returned: returned_tx,
            error: error.clone(),
            map: None,
        };
        (
            Self {
                queue,
                retired,
                returned,
                stop,
                error,
                recovered: false,
            },
            callback,
        )
    }

    pub fn run<S>(
        &mut self,
        config: &PlaybackConfig,
        callback: PlaybackCallback,
        started: &mpsc::SyncSender<std::result::Result<String, String>>,
        open: impl FnOnce(&PlaybackConfig, PlaybackCallback) -> Result<(S, String)>,
    ) -> Result<()> {
        let (stream, name) = open(config, callback)?;
        let _ = started.send(Ok(name));
        while !self.stop.load(Ordering::Acquire) && !self.error.load(Ordering::Acquire) {
            self.retired.reclaim();
            if self.returned.is_abandoned() {
                self.error.store(true, Ordering::Release);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        self.stop.store(true, Ordering::Release);
        // Barrier against an admission already inside its critical section.
        drop(self.queue.lock().unwrap_or_else(|error| error.into_inner()));
        drop(stream);
        self.recover()?;
        anyhow::ensure!(
            !self.error.load(Ordering::Acquire),
            "audio output callback/device failed"
        );
        Ok(())
    }

    fn recover(&mut self) -> Result<()> {
        if self.recovered {
            return Ok(());
        }
        self.stop.store(true, Ordering::Release);
        drop(self.queue.lock().unwrap_or_else(|error| error.into_inner()));
        let consumer = loop {
            if let Ok(consumer) = self.returned.pop() {
                break consumer;
            }
            if self.returned.is_abandoned() {
                break self.returned.pop().map_err(|_| {
                    anyhow::anyhow!("playback callback did not return its consumer")
                })?;
            }
            self.retired.reclaim();
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        consumer.discard();
        self.retired.reclaim();
        self.recovered = true;
        Ok(())
    }
}
impl Drop for PlaybackWorker {
    fn drop(&mut self) {
        // Covers startup errors and unwind after the stream's own Drop.
        let _ = self.recover();
    }
}
