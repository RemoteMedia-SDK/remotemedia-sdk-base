//! Shared room history, mention gating and publication helpers for native assistants.
use remotemedia_plugin_sdk::{
    traits::{CancelToken, NodeRuntimeContextRead, Retention},
    types::RuntimeData,
};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
const NODE_TYPE: &str = "AssistantRoom";

/// Lines kept for context between replies.
pub const MAX_PENDING: usize = 24;
/// Words that do not make a line a question ("hey Pixel", "ok Pixel").
const FILLER: &[&str] = &[
    "hey", "hi", "hello", "ok", "okay", "so", "um", "uh", "yo", "excuse", "me",
];

#[derive(Default)]
pub struct Room {
    /// "name: line" since the last reply.
    pub pending: VecDeque<String>,
    /// The last lines, replies included.
    pub recent: VecDeque<String>,
    pub replies: u64,
    pub attributed: VecDeque<Value>,
    /// Speakers who named the assistant without a question yet: when, and
    /// what they said.
    pub waiting: HashMap<String, (Instant, String)>,
    /// The reply the room last said to stop.
    pub stopped: Option<String>,
}

/// What a reply has generated so far. Dropped before the reply ended (the
/// runtime cancelled the call on a barge), it leaves that text in the room's
/// recent lines, marked, for the conversation that is started next — unless
/// the cancel's disposition discards partial output.
pub struct Partial<'a> {
    pub room: &'a std::sync::Mutex<Room>,
    pub name: &'a str,
    pub text: String,
    pub ended: bool,
    pub cancel: CancelToken,
}

/// How a reply that was cut off is kept in the context.
pub fn cut_off_line(name: &str, text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| format!("{name}: {text} [interrupted before finishing]"))
}

impl Drop for Partial<'_> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        if self
            .cancel
            .cancellation()
            .is_some_and(|c| c.disposition.partial_output == Retention::Discard)
        {
            eprintln!("[{NODE_TYPE}] reply cut off; rolled back (the cancel discards it)");
            return;
        }
        if let (Some(line), Ok(mut room)) = (cut_off_line(self.name, &self.text), self.room.lock())
        {
            eprintln!(
                "[{NODE_TYPE}] reply cut off after {} characters; kept in the context, marked",
                self.text.trim().chars().count()
            );
            room.note(line);
            room.attributed.push_back(json!({"author": self.name, "role": "agent", "text": self.text.trim().chars().take(4000).collect::<String>(), "interrupted": true}));
            while room.attributed.len() > MAX_PENDING {
                room.attributed.pop_front();
            }
        }
    }
}

impl Room {
    /// Structured history for RAG; never infer author roles from generated text.
    pub fn note_attributed(&mut self, author: &str, text: &str, agent: bool, interrupted: bool) {
        self.note(format!("{author}: {text}"));
        self.attributed.push_back(
            json!({"author": author.chars().take(256).collect::<String>(),
            "role": if agent {"agent"} else {"person"},
            "text": text.chars().take(4000).collect::<String>(), "interrupted": interrupted}),
        );
        while self.attributed.len() > MAX_PENDING {
            self.attributed.pop_front();
        }
    }

    /// Adds a "name: line" to the context.
    pub fn note(&mut self, line: String) {
        for lines in [&mut self.pending, &mut self.recent] {
            lines.push_back(line.clone());
            while lines.len() > MAX_PENDING {
                lines.pop_front();
            }
        }
    }

    /// A person's live line. Returns what they said to the assistant, if the
    /// line is for it: the line itself, or the line after their bare name
    /// within `follow_up`.
    pub fn hear(
        &mut self,
        speaker: &str,
        text: &str,
        addressed: &Addressed,
        follow_up: Duration,
        now: Instant,
    ) -> Option<String> {
        self.note_attributed(speaker, text, false, false);
        let follow_up = self
            .waiting
            .remove(speaker)
            .filter(|(at, _)| now.duration_since(*at) <= follow_up)
            .map(|(_, name)| format!("{name} {text}"));
        match addressed {
            Addressed::Yes => Some(text.to_string()),
            Addressed::NameOnly => {
                self.waiting
                    .insert(speaker.to_string(), (now, text.to_string()));
                None
            }
            Addressed::No => follow_up,
        }
    }

    /// A person's history line: context, and the speaker is waiting for
    /// their question from `now` if the line is only the name, and not
    /// waiting after any other line.
    pub fn recall(&mut self, speaker: &str, text: &str, addressed: &Addressed, now: Instant) {
        self.note_attributed(speaker, text, false, false);
        if matches!(addressed, Addressed::NameOnly) {
            self.waiting
                .insert(speaker.to_string(), (now, text.to_string()));
        } else {
            self.waiting.remove(speaker);
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Addressed {
    No,
    /// Only the name: wait for the question.
    NameOnly,
    Yes,
}

/// `participant.name` when the client set one, else the peer id without the
/// page's `-xxxx` suffix.
pub fn speaker_name(ctx: &dyn NodeRuntimeContextRead) -> String {
    let Some(origin) = ctx.origin() else {
        return "Someone".into();
    };
    if let Some(name) = origin
        .fields()
        .get("participant.name")
        .filter(|n| !n.is_empty())
    {
        return name.clone();
    }
    let id = origin.id();
    match id.rsplit_once('-') {
        Some((name, tag)) if tag.len() == 4 && tag.chars().all(|c| c.is_ascii_hexdigit()) => {
            name.to_string()
        }
        _ => id.to_string(),
    }
}

/// A room agent's name for the author of a frame: the display name the room
/// gave, else as `speaker_name`.
pub fn room_speaker_name(ctx: &dyn NodeRuntimeContextRead) -> String {
    ctx.origin()
        .and_then(|origin| {
            origin
                .fields()
                .get("participant.display_name")
                .filter(|n| !n.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| speaker_name(ctx))
}

/// What a room agent reads from a participant link.
#[derive(Debug, PartialEq)]
pub enum RoomFrame {
    /// A room-log line; `agent` is its `author_kind`.
    Line {
        text: String,
        seq: Option<u64>,
        history: bool,
        agent: bool,
    },
    HistoryEnd,
    /// The room no longer wants this reply.
    Stop(String),
}

/// The frame in `data`, if it is one the assistant acts on. A `floor`
/// refusal for `floor_busy` is not: its text is still posted.
pub fn room_frame(data: &RuntimeData) -> Option<RoomFrame> {
    let v = match data {
        RuntimeData::Json(v) => v,
        _ => {
            return line_text(data).map(|text| RoomFrame::Line {
                text,
                seq: None,
                history: false,
                agent: false,
            })
        }
    };
    let stop = || {
        v["reply"]
            .as_str()
            .map(|id| RoomFrame::Stop(id.to_string()))
    };
    match v["kind"].as_str()? {
        "text" | "voice" => {
            let text = v["text"].as_str()?.trim();
            (!text.is_empty()).then(|| RoomFrame::Line {
                text: text.to_string(),
                seq: v["seq"].as_u64(),
                history: v["history"] == true,
                agent: v["author_kind"] == "agent",
            })
        }
        "history_end" => Some(RoomFrame::HistoryEnd),
        "reply_interrupted" => stop(),
        _ => None,
    }
}

/// A room agent's publications.
pub fn presence_listening(to: &str) -> Value {
    json!({ "kind": "presence", "state": "listening", "to": to })
}

/// `line` is the `seq` of the line the reply answers.
pub fn presence_thinking(reply: &str, to: &str, line: Option<u64>) -> Value {
    let mut presence = json!({ "kind": "presence", "state": "thinking", "reply": reply, "to": to });
    if let Some(seq) = line {
        presence["line"] = json!(seq);
    }
    presence
}

pub fn presence_idle(reply: &str) -> Value {
    json!({ "kind": "presence", "state": "idle", "reply": reply })
}

/// One sentence of a reply: the room posts `text`, text-to-speech speaks
/// `say`.
pub fn sentence_line(sentence: &str, reply: &str) -> Value {
    json!({ "kind": "text", "text": sentence, "say": sentence, "reply": reply })
}

/// Whether `text` names the assistant, and with more than the name.
pub fn addressed(wake_words: &[String], respond_all: bool, text: &str) -> Addressed {
    if respond_all {
        return Addressed::Yes;
    }
    let words: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    let is_wake = |w: &String| wake_words.iter().any(|wake| wake == w);
    if !words.iter().any(is_wake) {
        return Addressed::No;
    }
    let rest = words
        .iter()
        .filter(|w| !is_wake(w) && !FILLER.contains(&w.as_str()))
        .count();
    if rest <= 1 {
        Addressed::NameOnly
    } else {
        Addressed::Yes
    }
}

/// The words a line says: a transcript (Text) or a typed message.
pub fn line_text(data: &RuntimeData) -> Option<String> {
    match data {
        RuntimeData::Text(t) => Some(t.trim().to_string()),
        RuntimeData::Json(v) if v["kind"] == "text" => {
            v["text"].as_str().map(|t| t.trim().to_string())
        }
        _ => None,
    }
    .filter(|t| !t.is_empty())
}
