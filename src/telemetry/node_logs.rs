//! Per-node log capture (openspec: add-agent-harness-integration).
//!
//! Keeps the recent log records of every node of every session in memory so
//! an observer (the agent tool `rm_node_logs`) can fetch one node's errors,
//! warnings, info or debug lines without scraping process stderr.
//!
//! Records come from two places:
//!
//! - **Rust `tracing` events**, through [`NodeLogLayer`]. An event belongs to
//!   a node when it is emitted inside that node's [`node_span`] (the session
//!   router runs every node's initialization and tasks inside one), or when
//!   it carries both `session_id` and `node_id` fields.
//! - **Multiprocess Python workers**: the process manager pushes each stderr
//!   and stdout line with [`NodeLogStore::push`], the level parsed from the
//!   worker's `logging` format by [`parse_python_line`].
//!
//! Capture is off until a host calls [`NodeLogStore::enable`] (the CLI's
//! `remotemedia mcp` does), so other embedders pay nothing. Memory is bounded:
//! a ring per (node, level), so a flood of debug lines never evicts the
//! errors, and at most [`MAX_SESSIONS`] sessions are retained (ended ones
//! included, so a failed start can still be inspected).

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Name of the span that attributes events to a node.
pub const SPAN_NAME: &str = "rm_node";
/// Sessions retained (oldest first-seen evicted).
pub const MAX_SESSIONS: usize = 32;
/// Longest retained message, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 2048;

/// Records kept per node, per level (error, warn, info, debug, trace).
const RING_CAPS: [usize; 5] = [256, 256, 512, 512, 512];

/// The span a node's work runs in. Events inside it (and inside spans and
/// tasks entered from it) are attributed to `node_id` of `session_id`.
pub fn node_span(session_id: &str, node_id: &str) -> tracing::Span {
    tracing::info_span!(
        target: "remotemedia::node",
        "rm_node",
        rm_session = %session_id,
        rm_node = %node_id
    )
}

/// Log severity, most severe first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

impl LogLevel {
    pub const ALL: [LogLevel; 5] = [
        LogLevel::Error,
        LogLevel::Warn,
        LogLevel::Info,
        LogLevel::Debug,
        LogLevel::Trace,
    ];

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "error" | "err" | "critical" | "fatal" => LogLevel::Error,
            "warn" | "warning" => LogLevel::Warn,
            "info" | "log" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            "trace" => LogLevel::Trace,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }

    fn from_tracing(l: &tracing::Level) -> Self {
        match *l {
            tracing::Level::ERROR => LogLevel::Error,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::TRACE => LogLevel::Trace,
        }
    }

    fn to_filter(self) -> tracing::level_filters::LevelFilter {
        use tracing::level_filters::LevelFilter;
        match self {
            LogLevel::Error => LevelFilter::ERROR,
            LogLevel::Warn => LevelFilter::WARN,
            LogLevel::Info => LevelFilter::INFO,
            LogLevel::Debug => LevelFilter::DEBUG,
            LogLevel::Trace => LevelFilter::TRACE,
        }
    }
}

/// Where a record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogSource {
    /// A `tracing` event in the host process (Rust nodes, the runtime's
    /// handling of the node, multiprocess executor plumbing).
    Runtime,
    /// A line the node's worker process wrote to stderr.
    Stderr,
    /// A line the node's worker process wrote to stdout.
    Stdout,
}

impl LogSource {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "runtime" => LogSource::Runtime,
            "stderr" => LogSource::Stderr,
            "stdout" => LogSource::Stdout,
            _ => return None,
        })
    }
}

/// One captured log record.
#[derive(Debug, Clone, Serialize)]
pub struct LogRecord {
    /// Store-wide, increasing: pass the largest seen as `since_seq`.
    pub seq: u64,
    /// Unix time, milliseconds.
    pub ts_ms: u64,
    pub level: LogLevel,
    pub node_id: String,
    pub source: LogSource,
    /// `tracing` target (module path), or `python` for worker output.
    pub target: String,
    pub message: String,
}

#[derive(Default)]
struct NodeLogs {
    rings: [VecDeque<LogRecord>; 5],
    counts: [u64; 5],
}

struct SessionLogs {
    nodes: HashMap<String, NodeLogs>,
    last_seq: u64,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<String, SessionLogs>,
    /// First-seen order, oldest first.
    order: VecDeque<String>,
}

/// What to fetch from one session.
#[derive(Debug, Clone)]
pub struct LogQuery {
    /// `None` = every node.
    pub node: Option<String>,
    /// Levels to include, indexed by `LogLevel as usize`.
    pub levels: [bool; 5],
    pub source: Option<LogSource>,
    /// Case-insensitive substring of the message.
    pub contains: Option<String>,
    /// Only records after this sequence number (oldest first). `None` = the
    /// newest `limit` records.
    pub since_seq: Option<u64>,
    pub limit: usize,
}

impl LogQuery {
    /// Records at `min` severity or more severe.
    pub fn at_least(min: LogLevel) -> Self {
        let mut levels = [false; 5];
        for l in LogLevel::ALL {
            levels[l as usize] = l <= min;
        }
        Self {
            node: None,
            levels,
            source: None,
            contains: None,
            since_seq: None,
            limit: 100,
        }
    }
}

/// Records captured for one node, per level, since its session started
/// (carried on observer snapshots as `NodeTelemetry::logs`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LogCounts {
    pub error: u64,
    pub warn: u64,
    pub info: u64,
    pub debug: u64,
    pub trace: u64,
}

/// Per-node record counts, per level.
#[derive(Debug, Clone, Serialize)]
pub struct NodeLogSummary {
    pub node_id: String,
    /// Records captured since the session started, per level.
    pub counts: HashMap<&'static str, u64>,
    /// Records no longer retained (rotated out of the ring), per level.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub dropped: HashMap<&'static str, u64>,
}

/// A page of records.
#[derive(Debug, Clone, Serialize)]
pub struct LogPage {
    /// Matching records, oldest first.
    pub records: Vec<LogRecord>,
    /// Pass as `since_seq` to fetch what comes next.
    pub next_seq: u64,
    /// More matching records exist past this page (`since_seq` queries), or
    /// before it (tail queries).
    pub more: bool,
    pub nodes: Vec<NodeLogSummary>,
}

/// The process-wide store.
pub struct NodeLogStore {
    /// Capture level + 1; 0 = capture off.
    level: AtomicU8,
    seq: AtomicU64,
    inner: Mutex<Inner>,
}

impl Default for NodeLogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeLogStore {
    pub fn new() -> Self {
        Self {
            level: AtomicU8::new(0),
            seq: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// The store [`NodeLogLayer`] and the process manager write to.
    pub fn global() -> &'static NodeLogStore {
        static STORE: OnceLock<NodeLogStore> = OnceLock::new();
        STORE.get_or_init(NodeLogStore::new)
    }

    /// Capture records at `level` or more severe; `None` turns capture off.
    pub fn enable(&self, level: Option<LogLevel>) {
        let v = level.map(|l| l as u8 + 1).unwrap_or(0);
        self.level.store(v, Ordering::Relaxed);
    }

    /// The capture level, `None` when off.
    pub fn capture_level(&self) -> Option<LogLevel> {
        match self.level.load(Ordering::Relaxed) {
            0 => None,
            v => LogLevel::ALL.get(v as usize - 1).copied(),
        }
    }

    fn captures(&self, level: LogLevel) -> bool {
        let v = self.level.load(Ordering::Relaxed);
        v != 0 && (level as u8) < v
    }

    /// Record one line for `node_id` of `session_id` (dropped when capture is
    /// off or `level` is above the capture level).
    pub fn push(
        &self,
        session_id: &str,
        node_id: &str,
        level: LogLevel,
        source: LogSource,
        target: &str,
        message: &str,
    ) {
        if !self.captures(level) {
            return;
        }
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut inner = self.inner.lock();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let record = LogRecord {
            seq,
            ts_ms,
            level,
            node_id: node_id.to_string(),
            source,
            target: target.to_string(),
            message: truncate(message, MAX_MESSAGE_BYTES),
        };
        if !inner.sessions.contains_key(session_id) {
            while inner.order.len() >= MAX_SESSIONS {
                if let Some(old) = inner.order.pop_front() {
                    inner.sessions.remove(&old);
                }
            }
            inner.order.push_back(session_id.to_string());
            inner.sessions.insert(
                session_id.to_string(),
                SessionLogs {
                    nodes: HashMap::new(),
                    last_seq: 0,
                },
            );
        }
        let session = inner.sessions.get_mut(session_id).expect("inserted above");
        session.last_seq = seq;
        let node = session.nodes.entry(node_id.to_string()).or_default();
        let i = level as usize;
        node.counts[i] += 1;
        let ring = &mut node.rings[i];
        if ring.len() == RING_CAPS[i] {
            ring.pop_front();
        }
        ring.push_back(record);
    }

    /// How many records `node_id` of `session_id` produced, per level;
    /// `None` when it produced none (or capture is off).
    pub fn counts(&self, session_id: &str, node_id: &str) -> Option<LogCounts> {
        let inner = self.inner.lock();
        let c = inner.sessions.get(session_id)?.nodes.get(node_id)?.counts;
        Some(LogCounts {
            error: c[0],
            warn: c[1],
            info: c[2],
            debug: c[3],
            trace: c[4],
        })
    }

    /// Sessions with captured records, most recently first seen first.
    pub fn sessions(&self) -> Vec<String> {
        self.inner.lock().order.iter().rev().cloned().collect()
    }

    /// The most recently first-seen session.
    pub fn latest_session(&self) -> Option<String> {
        self.inner.lock().order.back().cloned()
    }

    /// Nodes of `session_id` with captured records.
    pub fn nodes(&self, session_id: &str) -> Vec<String> {
        let inner = self.inner.lock();
        let mut v: Vec<String> = inner
            .sessions
            .get(session_id)
            .map(|s| s.nodes.keys().cloned().collect())
            .unwrap_or_default();
        v.sort();
        v
    }

    /// Records of `session_id` matching `q`; `None` when nothing was ever
    /// captured for that session.
    pub fn query(&self, session_id: &str, q: &LogQuery) -> Option<LogPage> {
        let inner = self.inner.lock();
        let session = inner.sessions.get(session_id)?;
        let needle = q.contains.as_ref().map(|c| c.to_lowercase());
        let mut matched: Vec<&LogRecord> = session
            .nodes
            .iter()
            .filter(|(id, _)| q.node.as_ref().map(|n| n == *id).unwrap_or(true))
            .flat_map(|(_, n)| {
                n.rings
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| q.levels[*i])
                    .flat_map(|(_, r)| r.iter())
            })
            .filter(|r| q.source.map(|s| s == r.source).unwrap_or(true))
            .filter(|r| q.since_seq.map(|s| r.seq > s).unwrap_or(true))
            .filter(|r| {
                needle
                    .as_ref()
                    .map(|n| r.message.to_lowercase().contains(n))
                    .unwrap_or(true)
            })
            .collect();
        matched.sort_by_key(|r| r.seq);
        let limit = q.limit.max(1);
        let more = matched.len() > limit;
        let page: Vec<LogRecord> = if q.since_seq.is_some() {
            matched.into_iter().take(limit).cloned().collect()
        } else {
            let skip = matched.len().saturating_sub(limit);
            matched.into_iter().skip(skip).cloned().collect()
        };
        let next_seq = if q.since_seq.is_some() && more {
            page.last().map(|r| r.seq).unwrap_or(0)
        } else {
            session.last_seq.max(q.since_seq.unwrap_or(0))
        };
        let mut nodes: Vec<NodeLogSummary> = session
            .nodes
            .iter()
            .filter(|(id, _)| q.node.as_ref().map(|n| n == *id).unwrap_or(true))
            .map(|(id, n)| {
                let mut counts = HashMap::new();
                let mut dropped = HashMap::new();
                for l in LogLevel::ALL {
                    let i = l as usize;
                    if n.counts[i] > 0 {
                        counts.insert(l.as_str(), n.counts[i]);
                    }
                    let gone = n.counts[i] - n.rings[i].len() as u64;
                    if gone > 0 {
                        dropped.insert(l.as_str(), gone);
                    }
                }
                NodeLogSummary {
                    node_id: id.clone(),
                    counts,
                    dropped,
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Some(LogPage {
            records: page,
            next_seq,
            more,
            nodes,
        })
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// The level of one line a Python worker wrote. Understands the runner's
/// `logging` format (`<time> - <logger> - LEVEL - message`) and Python's
/// default `LEVEL:logger:message`; otherwise tracebacks and `...Error:` /
/// `...Exception:` lines are errors, `...Warning:` lines warnings, indented
/// continuation lines keep `prev`, and anything else is info.
pub fn parse_python_line(line: &str, prev: Option<LogLevel>) -> LogLevel {
    let level_word = |w: &str| -> Option<LogLevel> {
        match w.trim() {
            "CRITICAL" | "FATAL" | "ERROR" => Some(LogLevel::Error),
            "WARNING" | "WARN" => Some(LogLevel::Warn),
            "INFO" => Some(LogLevel::Info),
            "DEBUG" => Some(LogLevel::Debug),
            _ => None,
        }
    };
    if let Some(l) = line.split(" - ").skip(1).take(3).find_map(level_word) {
        return l;
    }
    if let Some((head, _)) = line.split_once(':') {
        if let Some(l) = level_word(head) {
            return l;
        }
    }
    let t = line.trim_start();
    if t.starts_with("Traceback (most recent call last)") {
        return LogLevel::Error;
    }
    let indented = t.len() != line.len();
    if indented {
        if let Some(p) = prev {
            return p;
        }
    }
    let first = t.split_whitespace().next().unwrap_or("");
    if first.ends_with("Error:") || first.ends_with("Exception:") || first.ends_with("Error") {
        return LogLevel::Error;
    }
    if first.ends_with("Warning:") || t.contains("Warning: ") {
        return LogLevel::Warn;
    }
    LogLevel::Info
}

// ── tracing integration ───────────────────────────────────────────────────

/// Which node a span belongs to (stored in the `rm_node` span's extensions).
#[derive(Clone)]
struct NodeTag {
    session: String,
    node: String,
}

#[derive(Default)]
struct SpanFields {
    session: Option<String>,
    node: Option<String>,
}

impl Visit for SpanFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "rm_session" => self.session = Some(value.to_string()),
            "rm_node" => self.node = Some(value.to_string()),
            _ => {}
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "rm_session" => self.session = Some(format!("{value:?}")),
            "rm_node" => self.node = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

#[derive(Default)]
struct EventFields {
    message: String,
    rest: String,
    session_id: Option<String>,
    node_id: Option<String>,
}

impl EventFields {
    fn put(&mut self, field: &Field, value: String) {
        match field.name() {
            "message" => self.message = value,
            "session_id" => self.session_id = Some(value),
            "node_id" => self.node_id = Some(value),
            name if name.starts_with("log.") => {}
            name => {
                if self.rest.len() < MAX_MESSAGE_BYTES {
                    let _ = write!(self.rest, " {name}={value}");
                }
            }
        }
    }
}

impl Visit for EventFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}"));
    }
}

/// Feeds node-attributed `tracing` events into a [`NodeLogStore`]. Add it
/// with its [`NodeLogLayer::filter`] so it sees debug events of nodes
/// whatever the console filter is:
///
/// ```ignore
/// let store = NodeLogStore::global();
/// store.enable(Some(LogLevel::Debug));
/// tracing_subscriber::registry()
///     .with(fmt::layer().with_filter(env_filter))
///     .with(NodeLogLayer::new(store).with_filter(NodeLogLayer::filter(store)))
///     .init();
/// ```
pub struct NodeLogLayer {
    store: &'static NodeLogStore,
}

impl NodeLogLayer {
    pub fn new(store: &'static NodeLogStore) -> Self {
        Self { store }
    }

    /// The per-layer filter: `rm_node` spans, and events at or above the
    /// store's capture level that belong to a node.
    pub fn filter(store: &'static NodeLogStore) -> NodeLogFilter {
        NodeLogFilter { store }
    }
}

impl<S> Layer<S> for NodeLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != SPAN_NAME {
            return;
        }
        let mut f = SpanFields::default();
        attrs.record(&mut f);
        if let (Some(session), Some(node), Some(span)) = (f.session, f.node, ctx.span(id)) {
            span.extensions_mut().insert(NodeTag { session, node });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        let level = LogLevel::from_tracing(meta.level());
        if !self.store.captures(level) {
            return;
        }
        let mut f = EventFields::default();
        event.record(&mut f);
        let tag = ctx
            .event_scope(event)
            .and_then(|scope| {
                scope
                    .into_iter()
                    .find_map(|s| s.extensions().get::<NodeTag>().cloned())
            })
            .or_else(|| match (f.session_id.take(), f.node_id.take()) {
                (Some(session), Some(node)) => Some(NodeTag { session, node }),
                _ => None,
            });
        let Some(tag) = tag else { return };
        let message = if f.rest.is_empty() {
            f.message
        } else {
            format!("{}{}", f.message, f.rest)
        };
        self.store.push(
            &tag.session,
            &tag.node,
            level,
            LogSource::Runtime,
            meta.target(),
            &message,
        );
    }
}

/// See [`NodeLogLayer::filter`].
pub struct NodeLogFilter {
    store: &'static NodeLogStore,
}

impl<S> Filter<S> for NodeLogFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        if meta.is_span() {
            return meta.name() == SPAN_NAME;
        }
        if !self.store.captures(LogLevel::from_tracing(meta.level())) {
            return false;
        }
        let fields = meta.fields();
        if fields.field("session_id").is_some() && fields.field("node_id").is_some() {
            return true;
        }
        cx.lookup_current()
            .map(|s| s.scope().any(|s| s.name() == SPAN_NAME))
            .unwrap_or(false)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        use tracing::subscriber::Interest;
        if meta.is_span() {
            return if meta.name() == SPAN_NAME {
                Interest::always()
            } else {
                Interest::never()
            };
        }
        // The capture level can change at runtime (`enable`), so events
        // are decided per call.
        Interest::sometimes()
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(
            self.store
                .capture_level()
                .map(LogLevel::to_filter)
                .unwrap_or(tracing::level_filters::LevelFilter::OFF),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    fn leaked_store(level: LogLevel) -> &'static NodeLogStore {
        let s: &'static NodeLogStore = Box::leak(Box::new(NodeLogStore::new()));
        s.enable(Some(level));
        s
    }

    #[test]
    fn events_in_a_node_span_are_attributed_to_that_node_by_level() {
        let store = leaked_store(LogLevel::Debug);
        let sub = tracing_subscriber::registry()
            .with(NodeLogLayer::new(store).with_filter(NodeLogLayer::filter(store)));
        tracing::subscriber::with_default(sub, || {
            let span = node_span("s1", "stt");
            let _g = span.enter();
            tracing::error!(code = 7, "model failed");
            tracing::warn!("slow chunk");
            tracing::info!("loaded");
            tracing::debug!("chunk 1");
            tracing::trace!("not captured");
            drop(_g);
            tracing::error!("outside any node");
            tracing::warn!(session_id = "s1", node_id = "mic", "by fields");
        });

        let all = store
            .query("s1", &LogQuery::at_least(LogLevel::Trace))
            .unwrap();
        let msgs: Vec<_> = all
            .records
            .iter()
            .map(|r| (r.node_id.as_str(), r.level, r.message.as_str()))
            .collect();
        assert_eq!(
            msgs,
            vec![
                ("stt", LogLevel::Error, "model failed code=7"),
                ("stt", LogLevel::Warn, "slow chunk"),
                ("stt", LogLevel::Info, "loaded"),
                ("stt", LogLevel::Debug, "chunk 1"),
                ("mic", LogLevel::Warn, "by fields"),
            ]
        );

        let mut q = LogQuery::at_least(LogLevel::Warn);
        q.node = Some("stt".into());
        let page = store.query("s1", &q).unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.nodes.len(), 1);
        assert_eq!(page.nodes[0].counts["debug"], 1);
    }

    #[test]
    fn a_debug_flood_does_not_evict_errors_and_since_seq_pages_forward() {
        let store = leaked_store(LogLevel::Debug);
        store.push(
            "s",
            "n",
            LogLevel::Error,
            LogSource::Stderr,
            "python",
            "boom",
        );
        for i in 0..2000 {
            store.push(
                "s",
                "n",
                LogLevel::Debug,
                LogSource::Runtime,
                "t",
                &format!("d{i}"),
            );
        }
        let errors = store
            .query("s", &LogQuery::at_least(LogLevel::Error))
            .unwrap();
        assert_eq!(errors.records.len(), 1);
        assert_eq!(errors.records[0].message, "boom");

        let mut q = LogQuery::at_least(LogLevel::Debug);
        q.limit = 10;
        let tail = store.query("s", &q).unwrap();
        assert_eq!(tail.records.last().unwrap().message, "d1999");
        assert!(tail.more);
        assert_eq!(tail.nodes[0].dropped["debug"], 2000 - 512);

        store.push(
            "s",
            "n",
            LogLevel::Info,
            LogSource::Stdout,
            "python",
            "after",
        );
        q.since_seq = Some(tail.next_seq);
        let next = store.query("s", &q).unwrap();
        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0].message, "after");
    }

    #[test]
    fn capture_off_and_level_threshold_drop_records() {
        let store: &'static NodeLogStore = Box::leak(Box::new(NodeLogStore::new()));
        store.push("s", "n", LogLevel::Error, LogSource::Stderr, "python", "x");
        assert!(store
            .query("s", &LogQuery::at_least(LogLevel::Trace))
            .is_none());
        store.enable(Some(LogLevel::Warn));
        store.push("s", "n", LogLevel::Info, LogSource::Stderr, "python", "x");
        store.push("s", "n", LogLevel::Warn, LogSource::Stderr, "python", "y");
        let p = store
            .query("s", &LogQuery::at_least(LogLevel::Trace))
            .unwrap();
        assert_eq!(p.records.len(), 1);
    }

    #[test]
    fn old_sessions_are_evicted() {
        let store = leaked_store(LogLevel::Info);
        for i in 0..(MAX_SESSIONS + 3) {
            store.push(
                &format!("s{i}"),
                "n",
                LogLevel::Info,
                LogSource::Runtime,
                "t",
                "m",
            );
        }
        assert!(store
            .query("s0", &LogQuery::at_least(LogLevel::Info))
            .is_none());
        assert_eq!(store.sessions().len(), MAX_SESSIONS);
        assert_eq!(
            store.latest_session().unwrap(),
            format!("s{}", MAX_SESSIONS + 2)
        );
    }

    #[test]
    fn python_lines_get_their_logging_level() {
        use LogLevel::*;
        let p = |l: &str, prev| parse_python_line(l, prev);
        assert_eq!(
            p(
                "2026-11-08 10:00:00,1 - remotemedia.stt - INFO - loaded",
                None
            ),
            Info
        );
        assert_eq!(p("2026-11-08 10:00:00,1 - x - WARNING - slow", None), Warn);
        assert_eq!(p("2026-11-08 10:00:00,1 - x - ERROR - bad", None), Error);
        assert_eq!(p("2026-11-08 10:00:00,1 - x - DEBUG - d", None), Debug);
        assert_eq!(p("WARNING:root:careful", None), Warn);
        assert_eq!(p("Traceback (most recent call last):", Some(Info)), Error);
        assert_eq!(
            p("  File \"x.py\", line 1, in <module>", Some(Error)),
            Error
        );
        assert_eq!(
            p("RuntimeError: ServiceInCorruptedState", Some(Error)),
            Error
        );
        assert_eq!(p("x.py:3: UserWarning: deprecated", None), Warn);
        assert_eq!(p("Downloading model.bin", None), Info);
    }
}
