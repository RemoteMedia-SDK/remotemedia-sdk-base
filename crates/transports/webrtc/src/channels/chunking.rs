//! Chunk splitting and reassembly for large data channel payloads.
//!
//! When a serialized `DataBuffer` exceeds [`super::MAX_CHUNK_SIZE`]
//! (128 KB), it is split into numbered chunks that are sent as
//! [`super::ChunkInfo`] frames over the data channel. The receiver
//! uses [`DataChannelReassembler`] to buffer and reassemble chunks
//! into the complete payload.
//!
//! # Design
//!
//! - **Splitter**: breaks a byte slice into bounded chunks, assigns
//!   stable `(stream_id, message_id)` identifiers, and numbers each
//!   chunk sequentially.
//! - **Reassembler**: buffers chunks by `(stream_id, message_id)`,
//!   handles out-of-order arrival and duplicates, reassembles when
//!   complete, and evicts stale entries to bound memory usage.
//!
//! # Invariants
//!
//! - Chunks within a message are ordered by `chunk_index`.
//! - The final chunk has `is_final = true`.
//! - `total_chunks` may be `None` for streaming mode (unknown total);
//!   completion is signaled by `is_final` instead.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use super::messages::{ChunkInfo, MAX_CHUNK_SIZE};

/// Default timeout for incomplete reassembly (30 seconds).
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum total memory for pending reassemblies (64 MB).
const DEFAULT_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Maximum number of pending reassemblies before eviction.
const DEFAULT_MAX_PENDING: usize = 128;

/// Key for identifying a message being reassembled.
type MessageKey = (String, String); // (stream_id, message_id)

/// State for a single in-progress reassembly.
struct PendingMessage {
    /// Chunks buffered by index.
    chunks: HashMap<u32, Vec<u8>>,
    /// Total expected chunks (if known).
    total_chunks: Option<u32>,
    /// Index of the final chunk (if received).
    max_index: u32,
    /// Whether the final chunk has arrived.
    is_complete: bool,
    /// Content type hint from the final chunk.
    content_type: Option<String>,
    /// Data type hint from the final chunk.
    data_type_hint: Option<String>,
    /// Time the first chunk arrived.
    created_at: Instant,
    /// Time the last chunk arrived.
    last_updated: Instant,
    /// Total bytes buffered so far.
    total_bytes: usize,
}

impl PendingMessage {
    fn new() -> Self {
        Self {
            chunks: HashMap::new(),
            total_chunks: None,
            max_index: 0,
            is_complete: false,
            content_type: None,
            data_type_hint: None,
            created_at: Instant::now(),
            last_updated: Instant::now(),
            total_bytes: 0,
        }
    }

    /// Add a chunk and return true if the message is now complete.
    fn insert(&mut self, index: u32, data: Vec<u8>, is_final: bool, total: Option<u32>) -> bool {
        // Skip duplicates
        if self.chunks.contains_key(&index) {
            return self.is_complete;
        }

        let data_len = data.len();
        self.total_chunks = self.total_chunks.or(total);
        self.chunks.insert(index, data);
        self.total_bytes += data_len;

        if index > self.max_index {
            self.max_index = index;
        }

        if is_final {
            self.is_complete = true;
        } else if let Some(total) = self.total_chunks {
            // Complete if we have all chunks 0..total-1
            self.is_complete = self.chunks.len() >= total as usize;
        }

        self.last_updated = Instant::now();
        self.is_complete
    }

    /// Reassemble chunks in order. Returns None if not complete.
    fn take_reassembled(&mut self) -> Option<Vec<u8>> {
        if !self.is_complete {
            return None;
        }

        let mut result = Vec::with_capacity(self.total_bytes);
        for i in 0..=self.max_index {
            match self.chunks.remove(&i) {
                Some(data) => result.extend(data),
                None => {
                    // Gap in sequence - not actually complete yet.
                    // Chunks already consumed; mark incomplete so the
                    // caller knows to wait for more chunks.
                    self.is_complete = false;
                    return None;
                }
            }
        }

        // Extract metadata from the last chunk's info before clearing
        let _content_type = self.content_type.take();
        let _data_type_hint = self.data_type_hint.take();

        self.total_bytes = 0;
        self.max_index = 0;
        Some(result)
    }

    /// Check if this message has timed out.
    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_updated.elapsed() > timeout
    }
}

/// Splits a byte slice into bounded chunks for data channel transmission.
///
/// # Example
///
/// ```
/// use remotemedia_webrtc::channels::{ChunkInfo, DataChannelSplitter};
///
/// let splitter = DataChannelSplitter::new("my-stream", "msg-123");
/// let large_payload = vec![0u8; 200_000];
/// let chunks: Vec<ChunkInfo> = splitter.split(&large_payload);
///
/// assert!(chunks.len() > 1);
/// assert!(chunks.last().map_or(false, |c| c.is_final));
/// ```
pub struct DataChannelSplitter {
    /// Logical stream identifier.
    stream_id: String,
    /// Unique message identifier within the stream.
    message_id: String,
    /// Maximum size of each chunk in bytes.
    max_chunk_size: usize,
}

impl DataChannelSplitter {
    /// Create a new splitter with the default chunk size.
    pub fn new(stream_id: impl Into<String>, message_id: impl Into<String>) -> Self {
        Self {
            stream_id: stream_id.into(),
            message_id: message_id.into(),
            max_chunk_size: MAX_CHUNK_SIZE,
        }
    }

    /// Create a splitter with a custom max chunk size.
    pub fn with_max_chunk_size(
        stream_id: impl Into<String>,
        message_id: impl Into<String>,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            message_id: message_id.into(),
            max_chunk_size,
        }
    }

    /// Split a payload into chunks.
    ///
    /// If the payload fits in a single chunk, returns a single-element
    /// vector with `is_final = true` and `total_chunks = Some(1)`.
    ///
    /// # Arguments
    ///
    /// * `data` - The payload bytes to split.
    ///
    /// # Returns
    ///
    /// A vector of [`ChunkInfo`] frames, or an empty vector if the
    /// input is empty.
    pub fn split(&self, data: &[u8]) -> Vec<ChunkInfo> {
        if data.is_empty() {
            return Vec::new();
        }

        let chunk_size = self.max_chunk_size;
        let total_chunks = data.len().div_ceil(chunk_size);

        let mut chunks = Vec::with_capacity(total_chunks);
        let mut index = 0u32;

        for chunk_bytes in data.chunks(chunk_size) {
            let is_final = index + 1 >= total_chunks as u32;

            let chunk = ChunkInfo::new(
                &self.stream_id,
                &self.message_id,
                index,
                Some(total_chunks as u32),
                is_final,
                chunk_bytes.to_vec(),
            );

            chunks.push(chunk);
            index += 1;
        }

        chunks
    }

    /// Check if the data needs chunking.
    pub fn needs_chunking(&self, data: &[u8]) -> bool {
        data.len() > self.max_chunk_size
    }
}

impl Default for DataChannelSplitter {
    fn default() -> Self {
        Self::new("default", uuid::Uuid::new_v4().to_string())
    }
}

/// Reassembles chunked data channel messages into complete payloads.
///
/// Buffers chunks by `(stream_id, message_id)` and emits complete
/// payloads when all chunks arrive. Handles out-of-order delivery,
/// duplicate chunks, and timeouts.
///
/// # Memory Management
///
/// - Evicts entries older than the timeout (default 30s).
/// - Caps total pending memory at a configurable limit (default 64 MB).
/// - Caps the number of pending reassemblies (default 128).
///
/// # Example
///
/// ```
/// use remotemedia_webrtc::channels::{ChunkInfo, DataChannelReassembler, DataChannelSplitter};
///
/// let mut reassembler = DataChannelReassembler::new();
/// let splitter = DataChannelSplitter::new("stream", "msg");
///
/// let payload = vec![1, 2, 3, 4, 5];
/// let chunks: Vec<ChunkInfo> = splitter.split(&payload);
///
/// let mut result = None;
/// for chunk in &chunks {
///     if let Some(reassembled) = reassembler.feed_chunk(chunk) {
///         result = Some(reassembled);
///     }
/// }
///
/// assert_eq!(result, Some(payload));
/// ```
pub struct DataChannelReassembler {
    /// Pending reassemblies keyed by (stream_id, message_id).
    pending: HashMap<MessageKey, PendingMessage>,
    /// Timeout for incomplete reassemblies.
    timeout: Duration,
    /// Maximum total bytes across all pending reassemblies.
    max_memory_bytes: usize,
    /// Maximum number of pending reassemblies.
    max_pending: usize,
    /// Current total bytes in pending reassemblies.
    current_memory_bytes: usize,
    /// Optional channel to emit reassembled payloads.
    output_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

impl DataChannelReassembler {
    /// Create a new reassembler with default settings.
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_pending: DEFAULT_MAX_PENDING,
            current_memory_bytes: 0,
            output_tx: None,
        }
    }

    /// Set the reassembly timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum memory for pending reassemblies.
    #[must_use]
    pub fn with_max_memory(mut self, max_bytes: usize) -> Self {
        self.max_memory_bytes = max_bytes;
        self
    }

    /// Set an output channel for completed payloads.
    #[must_use]
    pub fn with_output(mut self, tx: mpsc::UnboundedSender<Vec<u8>>) -> Self {
        self.output_tx = Some(tx);
        self
    }

    /// Feed a chunk into the reassembler.
    ///
    /// Returns `Some(payload)` if the message is complete, `None` if
    /// more chunks are needed, or `None` if the chunk was a duplicate.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk info to process.
    pub fn feed_chunk(&mut self, chunk: &ChunkInfo) -> Option<Vec<u8>> {
        let key = (chunk.stream_id.clone(), chunk.message_id.clone());

        // Evict expired entries before inserting new ones
        if self.pending.len() >= self.max_pending {
            self.evict_expired();
            // If still over limit, evict the oldest entry
            if self.pending.len() >= self.max_pending {
                self.evict_oldest();
            }
        }

        // Check memory limit
        if self.current_memory_bytes + chunk.data.len() > self.max_memory_bytes {
            warn!(
                "Reassembly memory limit exceeded ({} + {} > {}), evicting oldest",
                self.current_memory_bytes,
                chunk.data.len(),
                self.max_memory_bytes
            );
            self.evict_oldest();
        }

        let pending = self
            .pending
            .entry(key.clone())
            .or_insert_with(PendingMessage::new);

        let was_complete = pending.insert(
            chunk.chunk_index,
            chunk.data.clone(),
            chunk.is_final,
            chunk.total_chunks,
        );

        // Update metadata from chunk hints
        if let Some(ref ct) = chunk.content_type {
            pending.content_type = Some(ct.clone());
        }
        if let Some(ref dt) = chunk.data_type_hint {
            pending.data_type_hint = Some(dt.clone());
        }

        if was_complete {
            trace!(
                stream_id = %chunk.stream_id,
                message_id = %chunk.message_id,
                "Reassembled complete message"
            );

            if let Some(payload) = pending.take_reassembled() {
                let payload_size = payload.len();
                self.current_memory_bytes = self
                    .current_memory_bytes
                    .saturating_sub(pending.total_bytes);
                self.pending.remove(&key);

                debug!(
                    stream_id = %chunk.stream_id,
                    message_id = %chunk.message_id,
                    bytes = payload_size,
                    "Emitting reassembled payload"
                );

                // Send to output channel if configured
                if let Some(ref tx) = self.output_tx {
                    if let Err(e) = tx.send(payload.clone()) {
                        warn!("Failed to send reassembled payload: {}", e);
                    }
                }

                return Some(payload);
            }
        }

        None
    }

    /// Evict expired reassemblies.
    ///
    /// Returns the number of entries evicted.
    pub fn evict_expired(&mut self) -> usize {
        let before = self.pending.len();
        self.pending.retain(|key, pending| {
            if pending.is_expired(self.timeout) {
                warn!("Evicting expired reassembly: ({}, {})", key.0, key.1);
                self.current_memory_bytes = self
                    .current_memory_bytes
                    .saturating_sub(pending.total_bytes);
                false
            } else {
                true
            }
        });
        before - self.pending.len()
    }

    /// Evict the oldest pending reassembly.
    fn evict_oldest(&mut self) {
        if let Some(key) = self
            .pending
            .iter()
            .min_by_key(|(_, v)| v.created_at)
            .map(|((stream_id, message_id), _)| (stream_id.clone(), message_id.clone()))
        {
            if let Some(pending) = self.pending.remove(&key) {
                warn!("Evicting oldest reassembly: ({}, {})", key.0, key.1);
                self.current_memory_bytes = self
                    .current_memory_bytes
                    .saturating_sub(pending.total_bytes);
            }
        }
    }

    /// Get the number of pending reassemblies.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the total memory used by pending reassemblies.
    pub fn memory_used(&self) -> usize {
        self.current_memory_bytes
    }
}

impl Default for DataChannelReassembler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_splits_large_payload() {
        let splitter = DataChannelSplitter::new("stream-1", "msg-1");
        // Create a payload larger than MAX_CHUNK_SIZE
        let data = vec![0xABu8; MAX_CHUNK_SIZE + 1000];
        let chunks = splitter.split(&data);

        assert!(!chunks.is_empty());
        assert!(chunks.len() > 1);

        // First chunks should not be final
        assert!(!chunks[0].is_final);
        // Last chunk should be final
        assert!(chunks.last().unwrap().is_final);

        // All chunks should have the same stream/message ID
        for chunk in &chunks {
            assert_eq!(chunk.stream_id, "stream-1");
            assert_eq!(chunk.message_id, "msg-1");
            assert_eq!(chunk.total_chunks, Some(chunks.len() as u32));
        }
    }

    #[test]
    fn splitter_single_chunk_small_payload() {
        let splitter = DataChannelSplitter::new("s", "m");
        let data = vec![1, 2, 3, 4, 5];
        let chunks = splitter.split(&data);

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_final);
        assert_eq!(chunks[0].total_chunks, Some(1));
        assert_eq!(chunks[0].data, data);
    }

    #[test]
    fn splitter_empty_payload() {
        let splitter = DataChannelSplitter::new("s", "m");
        let chunks = splitter.split(&[]);
        assert!(chunks.is_empty());
    }

    #[test]
    fn splitter_needs_chunking() {
        let splitter = DataChannelSplitter::new("s", "m");
        assert!(!splitter.needs_chunking(&vec![0u8; 1000]));
        assert!(splitter.needs_chunking(&vec![0u8; MAX_CHUNK_SIZE + 1]));
    }

    #[test]
    fn reassembler_reassembles_in_order() {
        let mut reassembler = DataChannelReassembler::new();
        let data = vec![1, 2, 3, 4, 5];

        let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 2);
        let chunks = splitter.split(&data);

        let mut result = Vec::new();
        for chunk in &chunks {
            if let Some(payload) = reassembler.feed_chunk(chunk) {
                result = payload;
            }
        }

        assert_eq!(result, data);
    }

    #[test]
    fn reassembler_out_of_order() {
        let mut reassembler = DataChannelReassembler::new();
        let data = vec![1, 2, 3, 4, 5, 6];

        let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 2);
        let chunks = splitter.split(&data);

        // Feed chunks out of order: 2, 0, 1
        let chunks: Vec<_> = chunks.into_iter().collect();
        let order = vec![1, 0, 2];

        let mut result = Vec::new();
        for idx in order {
            if let Some(payload) = reassembler.feed_chunk(&chunks[idx]) {
                result = payload;
            }
        }

        assert_eq!(result, data);
    }

    #[test]
    fn reassembler_duplicate_chunks() {
        let mut reassembler = DataChannelReassembler::new();
        let data = vec![1, 2, 3];

        let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 2);
        let chunks = splitter.split(&data);

        // Feed first chunk twice
        assert!(reassembler.feed_chunk(&chunks[0]).is_none());
        assert!(reassembler.feed_chunk(&chunks[0]).is_none());

        let result = reassembler.feed_chunk(&chunks[1]);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn reassembler_pending_count() {
        let mut reassembler = DataChannelReassembler::new();

        // Start a reassembly but don't complete it
        let chunk = ChunkInfo::new("s", "m1", 0, Some(3), false, vec![1, 2, 3]);
        reassembler.feed_chunk(&chunk);

        assert_eq!(reassembler.pending_count(), 1);

        // Complete the reassembly
        let chunk2 = ChunkInfo::new("s", "m1", 1, Some(3), false, vec![4, 5, 6]);
        let chunk3 = ChunkInfo::new("s", "m1", 2, Some(3), true, vec![7, 8, 9]);
        reassembler.feed_chunk(&chunk2);
        let result = reassembler.feed_chunk(&chunk3);

        assert!(result.is_some());
        assert_eq!(reassembler.pending_count(), 0);
    }

    #[test]
    fn reassembler_evict_expired() {
        let mut reassembler = DataChannelReassembler::new().with_timeout(Duration::from_millis(10));

        let chunk = ChunkInfo::new("s", "m1", 0, Some(2), false, vec![1, 2, 3]);
        reassembler.feed_chunk(&chunk);
        assert_eq!(reassembler.pending_count(), 1);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(20));

        let evicted = reassembler.evict_expired();
        assert_eq!(evicted, 1);
        assert_eq!(reassembler.pending_count(), 0);
    }

    #[test]
    fn splitter_and_reassembler_roundtrip() {
        let large_data: Vec<u8> = (0..=255).cycle().take(MAX_CHUNK_SIZE * 3 + 500).collect();

        let splitter = DataChannelSplitter::new("stream", "msg");
        let chunks = splitter.split(&large_data);

        let mut reassembler = DataChannelReassembler::new();
        let mut result = Vec::new();

        for chunk in &chunks {
            if let Some(payload) = reassembler.feed_chunk(chunk) {
                result = payload;
            }
        }

        assert_eq!(result, large_data);
    }
}
