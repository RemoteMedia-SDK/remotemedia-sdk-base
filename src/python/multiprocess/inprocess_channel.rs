//! Public in-process compatibility channel, available with `inprocess-python`.
//!
//! Each send is one immutable frame. Typed publish/receive uses counted portable
//! MessagePack; `send_owned`/`receive_owned_bytes` transfer existing SharedBytes
//! owners without payload copies. This API is not the main Python node runner.
//!
//! Capacity counts retained frames. With backpressure, a full queue rejects new
//! frames; without it, the oldest frame is evicted and lagging readers receive an
//! explicit error before resuming. Active readers release history once all have
//! consumed it. Before any reader attaches, bounded history supports late readers.

use crate::{Error, Result};
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason, RuntimeData, SharedBytes};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

mod state;
use state::Channel;

static GLOBAL_REGISTRY: OnceLock<Arc<InProcessChannelRegistry>> = OnceLock::new();

/// Best-effort diagnostic counters; CopyContext is the authoritative copy ledger.
#[derive(Debug, Default, Clone)]
pub struct ChannelStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_transferred: u64,
    pub last_activity: Option<std::time::Instant>,
}

#[derive(Debug, Clone)]
pub struct ChannelHandle {
    pub name: String,
    /// Maximum number of retained frames, not a payload byte limit.
    pub capacity: usize,
    pub stats: Arc<RwLock<ChannelStats>>,
    pub backpressure_enabled: bool,
}

pub struct InProcessChannelRegistry {
    channels: RwLock<HashMap<String, Arc<Channel>>>,
}

impl InProcessChannelRegistry {
    pub fn global() -> Arc<Self> {
        GLOBAL_REGISTRY
            .get_or_init(|| Arc::new(Self::new_internal()))
            .clone()
    }

    fn new_internal() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    pub async fn ensure_initialized(&self) -> Result<()> {
        Ok(())
    }

    /// Create a compatibility channel with its own AllowCopies ledger.
    pub async fn create_channel(
        &self,
        name: &str,
        capacity: usize,
        backpressure: bool,
    ) -> Result<ChannelHandle> {
        let copies = CopyContext::new(
            CopyPolicy::AllowCopies,
            format!("inprocess/channel/{name}/{}", uuid::Uuid::new_v4()),
        );
        self.create_channel_with_copy_context(name, capacity, backpressure, copies)
            .await
    }

    /// Bind one immutable authority for all publishers and subscribers. Reopening
    /// a live name is rejected, so another caller cannot replace its policy.
    pub async fn create_channel_with_copy_context(
        &self,
        name: &str,
        capacity: usize,
        backpressure: bool,
        copies: CopyContext,
    ) -> Result<ChannelHandle> {
        if capacity == 0 {
            return Err(ipc_error("channel capacity must be nonzero"));
        }
        let mut channels = self.channels.write().await;
        if channels.contains_key(name) {
            return Err(ipc_error("channel already exists"));
        }
        let channel = Arc::new(Channel::new(capacity, backpressure, copies));
        let handle = ChannelHandle {
            name: name.into(),
            capacity,
            stats: channel.stats.clone(),
            backpressure_enabled: backpressure,
        };
        channels.insert(name.into(), channel);
        Ok(handle)
    }

    /// Close old handles and release queued owners. A stale handle cannot destroy
    /// a replacement channel with the same name. Previously received owners live on.
    pub async fn destroy_channel(&self, handle: ChannelHandle) -> Result<()> {
        let mut channels = self.channels.write().await;
        if let Some(channel) = channels.get(&handle.name) {
            if !Arc::ptr_eq(&channel.stats, &handle.stats) {
                return Err(ipc_error("stale channel handle"));
            }
            channel.close()?;
            channels.remove(&handle.name);
        }
        Ok(())
    }

    /// Discard queued frames and return their total wire bytes. Readers that had
    /// not consumed those frames observe explicit lag, then can read new frames.
    pub async fn drain_channel(&self, name: &str) -> Result<usize> {
        let channels = self.channels.read().await;
        channels
            .get(name)
            .map(|channel| channel.drain())
            .transpose()
            .map(|n| n.unwrap_or(0))
    }

    async fn channel(&self, name: &str) -> Result<Arc<Channel>> {
        self.channels
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| ipc_error(format!("Channel not found: {name}")))
    }

    pub async fn create_publisher(&self, name: &str) -> Result<InProcessPublisher> {
        Ok(InProcessPublisher {
            channel: self.channel(name).await?,
        })
    }

    /// Replay retained history. Frames already consumed by all prior readers
    /// have been released and are not replayed.
    pub async fn create_subscriber(&self, name: &str) -> Result<InProcessSubscriber> {
        self.subscriber(name, false).await
    }

    /// Start at the current end without discarding another reader's backlog.
    pub async fn create_subscriber_fresh(&self, name: &str) -> Result<InProcessSubscriber> {
        self.subscriber(name, true).await
    }

    async fn subscriber(&self, name: &str, fresh: bool) -> Result<InProcessSubscriber> {
        let channel = self.channel(name).await?;
        let reader = channel.subscribe(fresh)?;
        Ok(InProcessSubscriber { channel, reader })
    }
}

pub struct InProcessPublisher {
    channel: Arc<Channel>,
}

impl InProcessPublisher {
    pub fn copy_context(&self) -> CopyContext {
        self.channel.copies.clone()
    }

    /// Encode directly into the final frame owner; there is no shared-Vec append.
    pub fn publish(&self, data: &RuntimeData) -> Result<()> {
        require_capacity(self.try_publish(data)?)
    }

    /// Return false on backpressure before serialization or allocation.
    pub fn try_publish(&self, data: &RuntimeData) -> Result<bool> {
        self.channel.enqueue(|| {
            super::data_transfer::to_bytes_with_copy_context(data, &self.channel.copies)
                .map(SharedBytes::from)
                .map_err(ipc_error)
        })
    }

    /// Copy a borrowed frame once, with exact wire-byte accounting.
    pub fn send(&self, bytes: &[u8]) -> Result<()> {
        require_capacity(self.channel.enqueue(|| {
            self.channel
                .copies
                .materialize(CopyReason::BorrowedInput, bytes.len() as u64, || {
                    SharedBytes::from(bytes.to_vec())
                })
                .map_err(ipc_error)
        })?)
    }

    /// Transfer an immutable frame owner, including slices, without copying.
    /// One call is one complete frame, not part of a byte-stream append.
    pub fn send_owned(&self, bytes: SharedBytes) -> Result<()> {
        require_capacity(self.channel.enqueue(|| Ok(bytes))?)
    }
}

pub struct InProcessSubscriber {
    channel: Arc<Channel>,
    reader: u64,
}

impl InProcessSubscriber {
    pub fn copy_context(&self) -> CopyContext {
        self.channel.copies.clone()
    }

    /// Decode exactly one complete portable frame. Malformed input, trailing
    /// bytes and strict rejection return errors without advancing this reader.
    pub fn receive(&mut self) -> Result<Option<RuntimeData>> {
        let Some((sequence, frame)) = self.channel.frame(self.reader)? else {
            return Ok(None);
        };
        let data = super::data_transfer::from_frame_with_copy_context(
            frame.as_slice(),
            &self.channel.copies,
        )
        .map_err(ipc_error)?;
        self.channel.advance(self.reader, sequence + 1, None)?;
        Ok(Some(data))
    }

    /// Receive one immutable frame with its backing storage retained. Strict mode
    /// permits this ownership transfer; decoded/Vec conversion remains explicit.
    pub fn receive_owned_bytes(&mut self) -> Result<Option<SharedBytes>> {
        let Some((sequence, frame)) = self.channel.frame(self.reader)? else {
            return Ok(None);
        };
        self.channel
            .advance(self.reader, sequence + 1, Some(frame.len()))?;
        Ok(Some(frame))
    }

    /// Legacy snapshot of all currently unread frames concatenated into one Vec.
    /// Copying is explicit; rejection leaves frames available to the owning API.
    pub fn receive_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let (next, frames) = self.channel.unread(self.reader)?;
        if frames.is_empty() {
            return Ok(None);
        }
        let size = state::wire_bytes(frames.iter())?;
        let bytes = self
            .channel
            .copies
            .materialize(CopyReason::VecMaterialization, size as u64, || {
                let mut output = Vec::with_capacity(size);
                for frame in frames {
                    output.extend_from_slice(frame.as_slice());
                }
                output
            })
            .map_err(ipc_error)?;
        self.channel.advance(self.reader, next, Some(size))?;
        Ok(Some(bytes))
    }
}

impl Drop for InProcessSubscriber {
    fn drop(&mut self) {
        self.channel.remove_reader(self.reader);
    }
}

fn ipc_error(message: impl std::fmt::Display) -> Error {
    Error::IpcError(message.to_string())
}
fn require_capacity(accepted: bool) -> Result<()> {
    if accepted {
        Ok(())
    } else {
        Err(ipc_error("in-process channel full"))
    }
}

#[cfg(test)]
mod baseline_tests;
#[cfg(test)]
mod tests;
