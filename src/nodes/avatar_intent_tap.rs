//! AvatarIntentTapNode — control-bus → NDJSON intent file bridge.
//!
//! Used by the WebRTC SPA to drive the avatar's locomotion / posture
//! state machine over the Session Control Bus. The browser publishes
//! intent JSON to `<id>.in`; this node validates the payload, appends
//! it as a single line to a configured NDJSON file, and emits the same
//! JSON on `<id>.out` so observer UIs can confirm acceptance.
//!
//! The standalone Bevy avatar prototype
//! (`examples/avatar-render-proto`) tails the same NDJSON via
//! `IntentWatcher`. The two halves are intentionally decoupled so
//! intent emission keeps working even when no avatar is connected.
//!
//! Validation is deliberately shallow: we require an `action: <string>`
//! field, leaving full schema enforcement to the consumer
//! (`examples/avatar-render-proto/src/intent.rs::Intent`). That keeps
//! the server from needing to track every new intent variant the
//! prototype gains.

use crate::data::RuntimeData;
use crate::nodes::SyncStreamingNode;
use crate::Error;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AvatarIntentTapConfig {
    /// Where to append validated intents (one JSON object per line).
    /// The standalone avatar prototype reads this same file via
    /// `--intents <path>`.
    pub intents_path: PathBuf,

    /// If true, truncate the file at node creation. Useful so old
    /// intents from a previous server run don't replay if the
    /// prototype starts with `--intents-replay-from-start`.
    /// Defaults to true.
    #[serde(default = "default_truncate")]
    pub truncate_on_start: bool,
}

fn default_truncate() -> bool {
    true
}

pub struct AvatarIntentTapNode {
    intents_path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl AvatarIntentTapNode {
    pub fn with_config(config: AvatarIntentTapConfig) -> Result<Self, Error> {
        if let Some(parent) = config.intents_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::Execution(format!(
                        "AvatarIntentTapNode: cannot create parent of {}: {e}",
                        config.intents_path.display()
                    ))
                })?;
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(!config.truncate_on_start)
            .truncate(config.truncate_on_start)
            .open(&config.intents_path)
            .map_err(|e| {
                Error::Execution(format!(
                    "AvatarIntentTapNode: cannot open {}: {e}",
                    config.intents_path.display()
                ))
            })?;

        tracing::info!(
            path = %config.intents_path.display(),
            truncated = config.truncate_on_start,
            "AvatarIntentTapNode ready"
        );

        Ok(Self {
            intents_path: config.intents_path,
            file: Mutex::new(file),
        })
    }

    fn parse_payload(data: &RuntimeData) -> Result<Value, String> {
        match data {
            RuntimeData::Json(v) => Ok(v.clone()),
            RuntimeData::Text(s) => serde_json::from_str::<Value>(s)
                .map_err(|e| format!("text payload is not JSON: {e}")),
            other => Err(format!(
                "expected Json or Text intent payload, got {:?}",
                other.data_type()
            )),
        }
    }

    fn validate(value: &Value) -> Result<&str, String> {
        let action = value
            .as_object()
            .and_then(|m| m.get("action"))
            .and_then(|a| a.as_str())
            .ok_or_else(|| "intent payload missing required string field 'action'".to_string())?;
        Ok(action)
    }

    fn append_line(&self, line: &str) -> Result<(), Error> {
        let mut f = self.file.lock();
        f.write_all(line.as_bytes()).map_err(|e| {
            Error::Execution(format!(
                "AvatarIntentTapNode: write to {} failed: {e}",
                self.intents_path.display()
            ))
        })?;
        f.write_all(b"\n").map_err(|e| {
            Error::Execution(format!(
                "AvatarIntentTapNode: write to {} failed: {e}",
                self.intents_path.display()
            ))
        })?;
        f.flush().map_err(|e| {
            Error::Execution(format!(
                "AvatarIntentTapNode: flush of {} failed: {e}",
                self.intents_path.display()
            ))
        })?;
        Ok(())
    }
}

impl SyncStreamingNode for AvatarIntentTapNode {
    fn node_type(&self) -> &str {
        "AvatarIntentTapNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "AvatarIntentTapNode requires streaming mode (use process_streaming)".into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let payload = match Self::parse_payload(&data) {
            Ok(v) => v,
            Err(e) => {
                // Demoted to debug — this fires for every audio packet
                // when the tap is wired downstream of a fanout that
                // includes audio (which is most pipelines), so keeping
                // it at warn floods the log.
                tracing::debug!("AvatarIntentTapNode: dropping payload — {e}");
                return Ok(0);
            }
        };

        let action = match Self::validate(&payload) {
            Ok(a) => a.to_string(),
            Err(e) => {
                tracing::warn!("AvatarIntentTapNode: invalid intent ({e}); payload={payload}");
                return Ok(0);
            }
        };

        let line = serde_json::to_string(&payload)
            .map_err(|e| Error::Execution(format!("intent re-serialize failed: {e}")))?;
        self.append_line(&line)?;
        tracing::info!(action = %action, "avatar intent appended");

        callback(RuntimeData::Json(payload))?;
        Ok(1)
    }
}
