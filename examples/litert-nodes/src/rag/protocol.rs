//! Strict, bounded service data and sentence publication shared by both backends.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;

pub const MAX_EVENT: usize = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub source_id: String,
    pub document_id: String,
    pub document_revision: String,
    pub chunk_id: String,
    pub index_version: String,
    pub title: String,
    pub line_start: u64,
    pub line_end: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub source: Source,
    pub text: String,
    pub score: f64,
}

pub fn digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn evidence(value: &Value, version: &str) -> Result<Vec<Evidence>, String> {
    let hits: Vec<Evidence> =
        serde_json::from_value(value.clone()).map_err(|_| "invalid evidence schema")?;
    if hits.len() > 8 || !digest(version) {
        return Err("invalid evidence count or snapshot".into());
    }
    let mut ids = HashSet::new();
    let mut chars = 0;
    for hit in &hits {
        let s = &hit.source;
        let id = s.source_id.as_bytes();
        let path = &s.document_id;
        let strings = [&s.source_id, path, &s.title];
        if id.len() != 2
            || id[0] != b'S'
            || !(b'1'..=b'8').contains(&id[1])
            || !ids.insert(&s.source_id)
            || strings
                .iter()
                .any(|v| v.is_empty() || v.chars().count() > 256)
            || path.contains(['\\', ':'])
            || path.chars().any(char::is_control)
            || path.split('/').any(|part| matches!(part, "" | "." | ".."))
            || !digest(&s.document_revision)
            || !digest(&s.chunk_id)
            || s.index_version != version
            || s.line_start == 0
            || s.line_end < s.line_start
            || hit.text.is_empty()
            || !hit.score.is_finite()
            || !(-1.0..=1.0).contains(&hit.score)
        {
            return Err("invalid evidence provenance".into());
        }
        chars += hit.text.chars().count();
    }
    let sources: Vec<&Source> = hits.iter().map(|hit| &hit.source).collect();
    if chars > 8000
        || serde_json::to_vec(&sources)
            .map_err(|_| "source encoding")?
            .len()
            > 8192
    {
        return Err("evidence exceeds bounds".into());
    }
    Ok(hits)
}

/// Correlation is validated on every event, before its text can reach the model or room.
pub fn correlated(event: &Value, request: &Value) -> bool {
    [
        "version",
        "request_id",
        "instance_id",
        "session_id",
        "member_id",
        "reply_id",
        "line_seq",
    ]
    .iter()
    .all(|key| event[*key] == request[*key])
        && digest(event["index_version"].as_str().unwrap_or(""))
}

/// Structural membership is necessary but does not establish factual entailment.
pub fn sentence(text: &str, hits: &[Evidence], reply: &str) -> Result<Value, String> {
    let re = regex::Regex::new(r"\[(S[1-8])\]").unwrap();
    let mut sources = Vec::new();
    for cap in re.captures_iter(text) {
        let hit = hits
            .iter()
            .find(|hit| hit.source.source_id == cap[1])
            .ok_or("unknown source ID")?;
        if !sources
            .iter()
            .any(|s: &&Source| s.source_id == hit.source.source_id)
        {
            sources.push(&hit.source);
        }
    }
    let plain = re.replace_all(text, "");
    if sources.is_empty()
        || plain.contains(['[', ']'])
        || ["<", ">", "```", "{", "}"]
            .iter()
            .any(|mark| plain.contains(mark))
    {
        return Err("sentence lacks valid citations or contains internal output".into());
    }
    let mut plain = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    for mark in [".", "!", "?", ",", ";", ":"] {
        plain = plain.replace(&format!(" {mark}"), mark);
    }
    if !plain.chars().any(char::is_alphanumeric) || plain.chars().count() > 500 {
        return Err("empty or oversized answer sentence".into());
    }
    Ok(json!({"kind":"text", "text":plain, "say":plain, "reply":reply, "sources":sources}))
}

/// Hold complete sentences (never a first clause); enforce a total output bound.
#[derive(Default)]
pub struct Sentences {
    pending: String,
    bytes: usize,
}
impl Sentences {
    pub fn push(&mut self, text: &str) -> Result<Vec<String>, String> {
        self.bytes += text.len();
        if self.bytes > 32_000 {
            return Err("answer exceeds output bound".into());
        }
        self.pending.push_str(text);
        let mut result = Vec::new();
        loop {
            let bytes = self.pending.as_bytes();
            let cut = bytes.iter().enumerate().find_map(|(i, c)| {
                (*c == b'\n'
                    || (matches!(c, b'.' | b'!' | b'?')
                        && bytes.get(i + 1).is_some_and(u8::is_ascii_whitespace)))
                .then_some(i + 1)
            });
            let Some(cut) = cut else { break };
            let line = self.pending[..cut].trim().to_owned();
            self.pending.drain(..cut);
            if !line.is_empty() {
                result.push(line);
            }
        }
        Ok(result)
    }
    pub fn finish(&mut self) -> Option<String> {
        let text = std::mem::take(&mut self.pending).trim().to_owned();
        (!text.is_empty()).then_some(text)
    }
}

/// Parse bounded UTF-8 SSE frames, including split multibyte sequences and CRLF.
#[derive(Default)]
pub struct Sse {
    pending: Vec<u8>,
}
impl Sse {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, String> {
        let mut events = Vec::new();
        for byte in bytes {
            if self.pending.len() >= MAX_EVENT {
                return Err("SSE frame exceeds limit".into());
            }
            self.pending.push(*byte);
            if self.pending.ends_with(b"\n\n") || self.pending.ends_with(b"\r\n\r\n") {
                let frame = std::str::from_utf8(&self.pending).map_err(|_| "invalid SSE UTF-8")?;
                let data = frame
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
                    .collect::<Vec<_>>()
                    .join("\n");
                if !data.is_empty() {
                    events.push(serde_json::from_str(&data).map_err(|_| "invalid SSE JSON")?);
                }
                self.pending.clear();
            }
        }
        Ok(events)
    }
    pub fn complete(&self) -> bool {
        self.pending.is_empty()
    }
}
