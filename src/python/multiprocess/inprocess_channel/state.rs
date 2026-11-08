use super::*;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

pub(super) struct Channel {
    pub stats: Arc<RwLock<ChannelStats>>,
    pub copies: CopyContext,
    capacity: usize,
    backpressure: bool,
    state: Mutex<History>,
}

#[derive(Default)]
struct History {
    frames: VecDeque<SharedBytes>,
    first: u64,
    next: u64,
    readers: HashMap<u64, u64>,
    next_reader: u64,
    closed: bool,
}

impl History {
    fn cursor(&mut self, reader: u64) -> Result<u64> {
        let cursor = self
            .readers
            .get_mut(&reader)
            .ok_or_else(|| ipc_error("unknown channel reader"))?;
        if *cursor < self.first {
            let lost = self.first - *cursor;
            *cursor = self.first;
            return Err(ipc_error(format!(
                "in-process subscriber missed {lost} frames"
            )));
        }
        Ok(*cursor)
    }

    fn prune(&mut self) {
        let consumed = self.readers.values().copied().min().unwrap_or(self.next);
        while self.first < consumed {
            self.frames.pop_front();
            self.first += 1;
        }
    }
}

impl Channel {
    pub fn new(capacity: usize, backpressure: bool, copies: CopyContext) -> Self {
        Self {
            stats: Arc::new(RwLock::new(ChannelStats::default())),
            copies,
            capacity,
            backpressure,
            state: Mutex::new(History::default()),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, History>> {
        self.state
            .lock()
            .map_err(|_| ipc_error("in-process channel state poisoned"))
    }

    pub fn enqueue(&self, build: impl FnOnce() -> Result<SharedBytes>) -> Result<bool> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(ipc_error("in-process channel closed"));
        }
        if state.frames.len() == self.capacity && self.backpressure {
            return Ok(false);
        }
        let next = state
            .next
            .checked_add(1)
            .ok_or_else(|| ipc_error("channel sequence exhausted"))?;
        // Build before eviction: failed conversions must not discard good data.
        let frame = build()?;
        let size = frame.len();
        if state.frames.len() == self.capacity {
            state.frames.pop_front();
            state.first += 1;
        }
        state.frames.push_back(frame);
        state.next = next;
        drop(state);
        if let Ok(mut stats) = self.stats.try_write() {
            stats.messages_sent += 1;
            stats.bytes_transferred += size as u64;
            stats.last_activity = Some(std::time::Instant::now());
        }
        Ok(true)
    }

    pub fn subscribe(&self, fresh: bool) -> Result<u64> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(ipc_error("in-process channel closed"));
        }
        let id = state.next_reader;
        state.next_reader = id
            .checked_add(1)
            .ok_or_else(|| ipc_error("channel reader IDs exhausted"))?;
        let cursor = if fresh { state.next } else { state.first };
        state.readers.insert(id, cursor);
        state.prune();
        Ok(id)
    }

    pub fn frame(&self, reader: u64) -> Result<Option<(u64, SharedBytes)>> {
        let mut state = self.lock()?;
        if state.closed {
            return Ok(None);
        }
        let cursor = state.cursor(reader)?;
        Ok(state
            .frames
            .get((cursor - state.first) as usize)
            .cloned()
            .map(|frame| (cursor, frame)))
    }

    pub fn unread(&self, reader: u64) -> Result<(u64, Vec<SharedBytes>)> {
        let mut state = self.lock()?;
        if state.closed {
            return Ok((state.next, Vec::new()));
        }
        let cursor = state.cursor(reader)?;
        Ok((
            state.next,
            state
                .frames
                .iter()
                .skip((cursor - state.first) as usize)
                .cloned()
                .collect(),
        ))
    }

    pub fn advance(&self, reader: u64, next: u64, wire_size: Option<usize>) -> Result<()> {
        let mut state = self.lock()?;
        let cursor = state
            .readers
            .get_mut(&reader)
            .ok_or_else(|| ipc_error("unknown channel reader"))?;
        *cursor = (*cursor).max(next);
        state.prune();
        drop(state);
        if let Ok(mut stats) = self.stats.try_write() {
            stats.messages_received += 1;
            stats.bytes_transferred += wire_size.unwrap_or(0) as u64;
            stats.last_activity = Some(std::time::Instant::now());
        }
        Ok(())
    }

    pub fn remove_reader(&self, reader: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.readers.remove(&reader);
            state.prune();
        }
    }

    pub fn drain(&self) -> Result<usize> {
        let mut state = self.lock()?;
        let size = wire_bytes(state.frames.iter())?;
        state.frames.clear();
        state.first = state.next;
        Ok(size)
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.lock()?;
        state.closed = true;
        state.frames.clear();
        state.first = state.next;
        Ok(())
    }
}

pub(super) fn wire_bytes<'a>(mut frames: impl Iterator<Item = &'a SharedBytes>) -> Result<usize> {
    frames.try_fold(0usize, |size, frame| {
        size.checked_add(frame.len())
            .ok_or_else(|| ipc_error("channel wire size overflow"))
    })
}
