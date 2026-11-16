//! Opt-in, content-free stage diagnostics for the room comparison harness.
//! Times are local elapsed durations, never synchronized host timestamps.
use serde_json::{json, Value};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

static NEXT: AtomicU64 = AtomicU64::new(1);
static LOST: AtomicU64 = AtomicU64::new(0);
static RECORD: AtomicU64 = AtomicU64::new(1);
static RUN: OnceLock<String> = OnceLock::new();

pub(crate) struct Stage {
    identity: Option<Value>,
    started: Instant,
    finished: bool,
}

impl Stage {
    pub(crate) fn new(stage: &str, reply: &str, details: Value) -> Self {
        let identity = (std::env::var("REMOTEMEDIA_RAG_TRACE").as_deref() == Ok("1")).then(|| {
            json!({"version":1,"stage":stage,"reply_id":reply,"process_id":std::process::id(),
                   "process_instance":RUN.get_or_init(|| uuid::Uuid::new_v4().to_string()),
                   "span_id":NEXT.fetch_add(1, Ordering::Relaxed),"clock":"local_elapsed_ms"})
        });
        let span = Self {
            identity,
            started: Instant::now(),
            finished: false,
        };
        span.mark("start", details);
        span
    }

    pub(crate) fn mark(&self, event: &str, details: Value) {
        if let Some(mut record) = self.identity.clone() {
            record["event"] = json!(event);
            record["record_id"] = json!(RECORD.fetch_add(1, Ordering::Relaxed));
            record["elapsed_ms"] = json!(self.started.elapsed().as_secs_f64() * 1000.0);
            record["details"] = details;
            record["lost_records"] = json!(LOST.load(Ordering::Relaxed));
            // Value's Display writes JSON in fragments. A host logger in another
            // DLL (or stdout redirected to the same file) has a different lock
            // and can split those fragments. Serialize before the single write.
            let line = format!("RAG_STAGE {record}\n");
            if std::io::stderr().lock().write_all(line.as_bytes()).is_err() {
                LOST.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn finish(mut self, details: Value) {
        self.finished = true;
        self.mark("end", details);
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if !self.finished {
            // Dropping can mean cancellation, error or a closed consumer. Do
            // not claim a more specific cause that this diagnostic cannot know.
            self.mark("end", json!({"outcome":"unfinished"}));
        }
    }
}
