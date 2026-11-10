//! Allocator-backed regular-file production. No detached decoder or bulk staging.

use super::{wav::WavInput, *};
use remotemedia_types::OutputAllocator;

impl FileIngestSource {
    pub(super) async fn start_direct(
        &mut self,
        allocator: Arc<dyn OutputAllocator>,
        max_frames: usize,
    ) -> Result<IngestStream, Error> {
        if self.stop_tx.is_some() || self.direct_task.is_some() {
            return Err(Error::ConfigError(
                "Source already started. Call stop() first.".into(),
            ));
        }
        if max_frames == 0 {
            return Err(Error::ConfigError(
                "File output frame bound must be positive".into(),
            ));
        }
        let path = self.path.clone();
        let config = self.config.clone();
        // Header discovery happens before installing a producer. Cancelling
        // this await cannot leave a producer allocating unpublished outputs.
        let input = tokio::task::spawn_blocking(move || WavInput::open(&path, &config, max_frames))
            .await
            .map_err(|e| Error::Other(format!("WAV discovery failed: {e}")))??;
        let metadata = input.metadata();
        *self.metadata.write().await = Some(metadata.clone());
        self.set_status(IngestStatus::Connected).await;
        let status = self.status.clone();
        let (stop_tx, stop_rx) = oneshot::channel();
        // One queued packet. Reserve before allocating so a stalled reader
        // cannot cause unbounded decode or unpublished output accumulation.
        let (data_tx, data_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);
        self.direct_task = Some(tokio::spawn(async move {
            let result = produce(input, allocator, data_tx, stop_rx).await;
            *status.write().await = match &result {
                Ok(()) => IngestStatus::Disconnected,
                Err(error) => IngestStatus::error(error.to_string()),
            };
            result
        }));
        Ok(IngestStream::new(data_rx, metadata))
    }
}

async fn produce(
    mut input: WavInput,
    allocator: Arc<dyn OutputAllocator>,
    data_tx: mpsc::Sender<RuntimeData>,
    mut stop_rx: oneshot::Receiver<()>,
) -> Result<(), Error> {
    while !input.is_empty() {
        let permit = tokio::select! {
            biased;
            _ = &mut stop_rx => return Ok(()),
            permit = data_tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return Ok(()),
            },
        };
        let provider = allocator.clone();
        // Always join the bounded blocking read, including during stop().
        // Moving the file/loan across tasks moves ownership, not payload bytes.
        let (next_input, packet) = tokio::task::spawn_blocking(move || {
            let packet = input.read_packet(provider.as_ref());
            (input, packet)
        })
        .await
        .map_err(|e| Error::Other(format!("WAV read task failed: {e}")))?;
        input = next_input;
        if !matches!(stop_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            return Ok(());
        }
        permit.send(packet?);
    }
    Ok(())
}
