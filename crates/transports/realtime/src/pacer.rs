//! The send queue of one connection (design D10, "Spoken text").
//!
//! The browser plays what it receives, so audio written to the socket is
//! audio the member will hear unless the browser truncates it. The pacer
//! therefore keeps at most `lead` of audio ahead of real time on the socket,
//! and accounts, per item, for what it has released. Audio still waiting here
//! can be dropped the moment the node cancels the item.
//!
//! The pacer does no I/O and reads no clock: the connection calls it with
//! the current instant, writes the [`Delta`]s it returns and passes the
//! [`Released`] reports to the node.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How many items and turns the pacer remembers after they ended.
const REMEMBERED: usize = 16;

/// Audio to write to the socket now, as one `audio-delta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub item_id: String,
    pub response_id: String,
    pub rate: u32,
    pub samples: Vec<i16>,
}

/// What has been released of an item: a `remotemedia.internal.released`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Released {
    pub item_id: String,
    pub response_id: String,
    /// Audio of the item written to the socket so far, in milliseconds.
    pub audio_ms: u64,
    /// Audio of the item received from the pipeline so far, in milliseconds
    /// of the session's output rate.
    pub received_ms: u64,
    /// Sent after a flush: the item's queued audio was dropped, and
    /// `audio_ms` is its final position.
    pub flushed: bool,
    /// Nothing of the item is waiting to be sent.
    pub drained: bool,
}

#[derive(Debug)]
struct Queued {
    item_id: String,
    response_id: String,
    rate: u32,
    samples: VecDeque<i16>,
}

#[derive(Debug)]
struct Item {
    item_id: String,
    response_id: String,
    /// Released and received audio, in microseconds.
    released_us: u64,
    received_us: u64,
    last_report: Option<Instant>,
    /// Audio was released since the last report.
    dirty: bool,
}

#[derive(Debug)]
pub struct Pacer {
    lead: Duration,
    /// The smallest piece worth a frame of its own, unless it is all there is.
    slice: Duration,
    /// The longest time between two reports of an item while it is sent.
    report_interval: Duration,
    queue: VecDeque<Queued>,
    /// When the browser will have played everything written so far, if it
    /// plays without a gap.
    ahead_until: Option<Instant>,
    items: VecDeque<Item>,
    /// Items and turns whose later audio is refused.
    dropped_items: VecDeque<String>,
    dropped_turns: VecDeque<String>,
}

fn duration_of(samples: usize, rate: u32) -> Duration {
    Duration::from_micros(samples as u64 * 1_000_000 / u64::from(rate.max(1)))
}

fn remember(list: &mut VecDeque<String>, value: &str) {
    if list.iter().any(|known| known == value) {
        return;
    }
    if list.len() >= REMEMBERED {
        list.pop_front();
    }
    list.push_back(value.to_string());
}

impl Pacer {
    pub fn new(lead: Duration, slice: Duration, report_interval: Duration) -> Self {
        Self {
            lead,
            slice: slice.min(lead).max(Duration::from_millis(1)),
            report_interval,
            queue: VecDeque::new(),
            ahead_until: None,
            items: VecDeque::new(),
            dropped_items: VecDeque::new(),
            dropped_turns: VecDeque::new(),
        }
    }

    pub fn lead(&self) -> Duration {
        self.lead
    }

    /// Whether audio is waiting to be sent.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn item_mut(&mut self, item_id: &str, response_id: &str) -> &mut Item {
        if let Some(at) = self.items.iter().position(|item| item.item_id == item_id) {
            return &mut self.items[at];
        }
        if self.items.len() >= REMEMBERED {
            self.items.pop_front();
        }
        self.items.push_back(Item {
            item_id: item_id.to_string(),
            response_id: response_id.to_string(),
            released_us: 0,
            received_us: 0,
            last_report: None,
            dirty: false,
        });
        self.items.back_mut().expect("item just added")
    }

    fn queued(&self, item_id: &str) -> bool {
        self.queue.iter().any(|queued| queued.item_id == item_id)
    }

    /// Queue audio of an item. `false` when the item (or its turn) was
    /// flushed and its later audio is refused.
    pub fn push(&mut self, item_id: &str, response_id: &str, rate: u32, samples: Vec<i16>) -> bool {
        if self.dropped_items.iter().any(|dropped| dropped == item_id)
            || self.dropped_turns.iter().any(|dropped| dropped == response_id)
        {
            return false;
        }
        if samples.is_empty() {
            return true;
        }
        self.item_mut(item_id, response_id).received_us +=
            duration_of(samples.len(), rate).as_micros() as u64;
        match self.queue.back_mut() {
            Some(last) if last.item_id == item_id && last.rate == rate => last.samples.extend(samples),
            _ => self.queue.push_back(Queued {
                item_id: item_id.to_string(),
                response_id: response_id.to_string(),
                rate,
                samples: samples.into(),
            }),
        }
        true
    }

    fn report(&mut self, at: usize, now: Instant, flushed: bool) -> Released {
        let drained = !self.queued(&self.items[at].item_id);
        let item = &mut self.items[at];
        item.last_report = Some(now);
        item.dirty = false;
        Released {
            item_id: item.item_id.clone(),
            response_id: item.response_id.clone(),
            audio_ms: item.released_us / 1_000,
            received_ms: item.received_us / 1_000,
            flushed,
            drained,
        }
    }

    /// What to write now, and the reports that are due.
    pub fn poll(&mut self, now: Instant) -> (Vec<Delta>, Vec<Released>) {
        let mut deltas: Vec<Delta> = Vec::new();
        loop {
            let ahead = self
                .ahead_until
                .map_or(Duration::ZERO, |until| until.saturating_duration_since(now));
            let budget = self.lead.saturating_sub(ahead);
            let Some(head) = self.queue.front_mut() else { break };
            let head_duration = duration_of(head.samples.len(), head.rate);
            // Wait until a slice fits, unless the rest is shorter than one.
            if budget < self.slice.min(head_duration) {
                break;
            }
            let fits = (budget.as_micros() as u64 * u64::from(head.rate) / 1_000_000) as usize;
            let take = fits.min(head.samples.len());
            if take == 0 {
                break;
            }
            let samples: Vec<i16> = head.samples.drain(..take).collect();
            let (item_id, response_id, rate) =
                (head.item_id.clone(), head.response_id.clone(), head.rate);
            if head.samples.is_empty() {
                self.queue.pop_front();
            }
            let sent = duration_of(take, rate);
            self.ahead_until = Some(self.ahead_until.map_or(now, |until| until.max(now)) + sent);
            let item = self.item_mut(&item_id, &response_id);
            item.released_us += sent.as_micros() as u64;
            item.dirty = true;
            match deltas.last_mut() {
                Some(last) if last.item_id == item_id && last.rate == rate => {
                    last.samples.extend(samples);
                }
                _ => deltas.push(Delta {
                    item_id,
                    response_id,
                    rate,
                    samples,
                }),
            }
        }

        // A report at least every interval while sending, and one as soon as
        // the last of an item has left.
        let mut reports = Vec::new();
        for at in 0..self.items.len() {
            let item = &self.items[at];
            if !item.dirty {
                continue;
            }
            // Half the interval: sends happen on a timer, and a report that
            // waited for the first send after the whole interval would be late.
            let due = item
                .last_report
                .is_none_or(|last| now.duration_since(last) >= self.report_interval / 2);
            if due || !self.queued(&item.item_id) {
                reports.push(self.report(at, now, false));
            }
        }
        (deltas, reports)
    }

    /// When [`Self::poll`] next has something to write. `None` with nothing
    /// queued.
    pub fn next_wake(&self, now: Instant) -> Option<Instant> {
        let head = self.queue.front()?;
        let needed = self.slice.min(duration_of(head.samples.len(), head.rate));
        let Some(until) = self.ahead_until else { return Some(now) };
        // budget(t) = lead - (until - t) >= needed
        let at = (until + needed).checked_sub(self.lead).unwrap_or(now);
        Some(at.max(now))
    }

    /// The browser discarded what it had: nothing written is ahead any more.
    fn reset_clock(&mut self, now: Instant) {
        self.ahead_until = Some(now);
    }

    /// Drop the queued audio of `item_id` (every item with `None`), refuse
    /// the later audio of those items, and report each one's final position.
    pub fn flush_item(&mut self, item_id: Option<&str>, now: Instant) -> Vec<Released> {
        let targets: Vec<String> = match item_id {
            Some(item_id) => vec![item_id.to_string()],
            None => {
                let mut items: Vec<String> = Vec::new();
                for queued in &self.queue {
                    if !items.contains(&queued.item_id) {
                        items.push(queued.item_id.clone());
                    }
                }
                for item in &self.items {
                    if item.dirty && !items.contains(&item.item_id) {
                        items.push(item.item_id.clone());
                    }
                }
                items
            }
        };
        let mut reports = Vec::new();
        for target in targets {
            self.queue.retain(|queued| queued.item_id != target);
            remember(&mut self.dropped_items, &target);
            // An item flushed before any of it arrived is reported at zero.
            let response_id = target.strip_suffix(":assistant").unwrap_or(&target).to_string();
            self.item_mut(&target, &response_id);
            let at = self
                .items
                .iter()
                .position(|item| item.item_id == target)
                .expect("flushed item is known");
            reports.push(self.report(at, now, true));
        }
        self.reset_clock(now);
        reports
    }

    /// Drop the queued audio of a turn (a targeted flush of the session,
    /// design D11) and report its items. With `drop_rest`, the turn's later
    /// audio is refused.
    pub fn flush_turn(&mut self, turn_id: &str, drop_rest: bool, now: Instant) -> Vec<Released> {
        let mut items: Vec<String> = Vec::new();
        for queued in &self.queue {
            if queued.response_id == turn_id && !items.contains(&queued.item_id) {
                items.push(queued.item_id.clone());
            }
        }
        for item in &self.items {
            if item.response_id == turn_id && item.dirty && !items.contains(&item.item_id) {
                items.push(item.item_id.clone());
            }
        }
        self.queue.retain(|queued| queued.response_id != turn_id);
        if drop_rest {
            remember(&mut self.dropped_turns, turn_id);
        }
        let mut reports = Vec::new();
        for target in items {
            if let Some(at) = self.items.iter().position(|item| item.item_id == target) {
                reports.push(self.report(at, now, true));
            }
        }
        self.reset_clock(now);
        reports
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RATE: u32 = 24_000;

    fn pacer() -> Pacer {
        Pacer::new(
            Duration::from_millis(500),
            Duration::from_millis(20),
            Duration::from_millis(100),
        )
    }

    fn audio(ms: u64) -> Vec<i16> {
        vec![1; (u64::from(RATE) * ms / 1000) as usize]
    }

    fn ms(deltas: &[Delta]) -> u64 {
        deltas
            .iter()
            .map(|delta| delta.samples.len() as u64 * 1000 / u64::from(delta.rate))
            .sum()
    }

    #[test]
    fn at_most_the_lead_is_sent_ahead_of_real_time() {
        let mut pacer = pacer();
        let start = Instant::now();
        assert!(pacer.push("t1:assistant", "t1", RATE, audio(3_000)));

        // The lead at once, as one delta.
        let (deltas, reports) = pacer.poll(start);
        assert_eq!(deltas.len(), 1);
        assert_eq!(ms(&deltas), 500);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].audio_ms, 500);
        assert!(!reports[0].drained);

        // Nothing more until real time has moved on by a slice.
        assert!(pacer.poll(start + Duration::from_millis(10)).0.is_empty());
        assert_eq!(pacer.next_wake(start), Some(start + Duration::from_millis(20)));

        // Then exactly what keeps the lead.
        let mut sent = 500;
        for tick in 1..=100u64 {
            let now = start + Duration::from_millis(tick * 20);
            sent += ms(&pacer.poll(now).0);
            assert!(sent <= tick * 20 + 500, "{sent} ms sent after {} ms", tick * 20);
            assert!(sent + 20 >= tick * 20 + 500, "fell behind: {sent} ms after {} ms", tick * 20);
        }
        assert_eq!(sent, 2_500);
        // The rest of the 3 s arrives over the next 0.5 s.
        sent += ms(&pacer.poll(start + Duration::from_millis(3_000)).0);
        assert_eq!(sent, 3_000);
        assert!(pacer.is_empty());
        assert_eq!(pacer.next_wake(start + Duration::from_millis(3_000)), None);
    }

    #[test]
    fn released_is_reported_at_least_every_interval_and_when_the_item_has_left() {
        let mut pacer = pacer();
        let start = Instant::now();
        pacer.push("t1:assistant", "t1", RATE, audio(1_000));
        let mut last_report = start;
        let mut reports = Vec::new();
        for tick in 0..=30u64 {
            let now = start + Duration::from_millis(tick * 20);
            for report in pacer.poll(now).1 {
                assert!(now.duration_since(last_report) <= Duration::from_millis(100));
                last_report = now;
                reports.push(report);
            }
        }
        let last = reports.last().expect("reports");
        assert_eq!(last.audio_ms, 1_000);
        assert_eq!(last.received_ms, 1_000);
        assert!(last.drained && !last.flushed);
        // Positions only grow.
        assert!(reports.windows(2).all(|pair| pair[0].audio_ms <= pair[1].audio_ms));
        // Nothing more once the item has left.
        assert!(pacer.poll(start + Duration::from_secs(5)).1.is_empty());
    }

    #[test]
    fn a_flush_drops_the_items_queued_audio_and_reports_the_final_position() {
        let mut pacer = pacer();
        let start = Instant::now();
        pacer.push("t1:assistant", "t1", RATE, audio(3_000));
        pacer.push("t2:assistant", "t2", RATE, audio(200));
        let mut sent = ms(&pacer.poll(start).0);
        sent += ms(&pacer.poll(start + Duration::from_millis(100)).0);
        assert_eq!(sent, 600);

        let flushed_at = start + Duration::from_millis(110);
        let reports = pacer.flush_item(Some("t1:assistant"), flushed_at);
        assert_eq!(
            reports,
            vec![Released {
                item_id: "t1:assistant".into(),
                response_id: "t1".into(),
                audio_ms: 600,
                received_ms: 3_000,
                flushed: true,
                drained: true,
            }]
        );

        // Nothing of the item is sent again, including audio that arrives late.
        assert!(!pacer.push("t1:assistant", "t1", RATE, audio(500)));
        let (deltas, _) = pacer.poll(flushed_at);
        assert!(deltas.iter().all(|delta| delta.item_id == "t2:assistant"));
        // The next item is not held back by audio the browser discarded.
        assert_eq!(ms(&deltas), 200);
    }

    #[test]
    fn an_item_flushed_before_its_audio_arrived_is_reported_at_zero() {
        let mut pacer = pacer();
        let now = Instant::now();
        let reports = pacer.flush_item(Some("t9:assistant"), now);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].audio_ms, 0);
        assert_eq!(reports[0].response_id, "t9");
        assert!(reports[0].flushed);
        assert!(!pacer.push("t9:assistant", "t9", RATE, audio(100)));
    }

    #[test]
    fn a_turn_flush_touches_only_that_turn() {
        let mut pacer = pacer();
        let start = Instant::now();
        pacer.push("t1:assistant", "t1", RATE, audio(2_000));
        pacer.push("t2:assistant", "t2", RATE, audio(300));
        pacer.poll(start);
        let reports = pacer.flush_turn("t1", true, start + Duration::from_millis(50));
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].item_id, "t1:assistant");
        assert_eq!(reports[0].audio_ms, 500);
        assert!(!pacer.push("t1:assistant", "t1", RATE, audio(100)));
        let (deltas, _) = pacer.poll(start + Duration::from_millis(50));
        assert_eq!(ms(&deltas), 300);
        assert_eq!(deltas[0].item_id, "t2:assistant");
    }

    #[test]
    fn after_a_pause_the_lead_is_available_again() {
        let mut pacer = pacer();
        let start = Instant::now();
        pacer.push("t1:assistant", "t1", RATE, audio(300));
        assert_eq!(ms(&pacer.poll(start).0), 300);
        // Played out long ago: the next item gets a full lead at once.
        let later = start + Duration::from_secs(5);
        pacer.push("t2:assistant", "t2", RATE, audio(2_000));
        assert_eq!(ms(&pacer.poll(later).0), 500);
    }
}
