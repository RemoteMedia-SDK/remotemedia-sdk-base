//! Folder-backed RAG room assistant. See scaling's rag_assistant_room_agent example.
mod client;
mod model;
mod prompt;
mod protocol;

use crate::{
    assistant::{config_from_params as assistant_config, AssistantConfig},
    assistant_room::*,
    lm::Piece,
    node::expand,
};
use async_trait::async_trait;
use protocol::{Evidence, Sentences};
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    traits::{
        rpc::CallContext, AsyncStreamingNode, CancelReason, CancelSource, CancelStrategy,
        Cancellable, Cancellation, GeneratorRole, NodeRuntimeContextRead, Retention, TextGenerator,
    },
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use serde_json::{json, Value};
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

pub const NODE_TYPE: &str = "RagAssistantNode";

#[derive(Clone)]
pub struct Config {
    pub assistant: AssistantConfig,
    pub server: bool,
    pub policy: String,
    pub url: String,
    pub token: String,
    pub collection: String,
    pub member: String,
    pub timeout_ms: u64,
    /// Launcher-only disposable compatibility session; never in the user's agent.
    pub source_probe: Option<(String, Vec<Evidence>)>,
}

pub fn config_from_params(params: &Value) -> Result<Config, String> {
    let s = |key: &str, default: &str| {
        params[key]
            .as_str()
            .map(expand)
            .unwrap_or_else(|| default.into())
    };
    let server = match s("answer_backend", "phone").as_str() {
        "phone" => false,
        "server" => true,
        _ => return Err("answer_backend must be phone or server".into()),
    };
    let policy = s("retrieval_policy", "direct");
    if !matches!(policy.as_str(), "direct" | "agentic") {
        return Err("retrieval_policy must be direct or agentic".into());
    }
    let url = s("rag_url", "").trim_end_matches('/').to_owned();
    let parsed = reqwest::Url::parse(&url).map_err(|_| "rag_url must be an HTTP(S) base URL")?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
    {
        return Err(
            "rag_url must be an HTTP(S) origin without credentials, path, query or fragment".into(),
        );
    }
    let token = s("rag_token", "");
    if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("rag_token is required (at most 512 ASCII characters)".into());
    }
    let mut common = params.clone();
    if server {
        common["model_path"] = json!("server-answer-no-local-model");
    }
    if common["publish"].is_null() {
        common["publish"] = json!("room");
    }
    if !params["follow_up_secs"].is_null()
        && !params["follow_up_secs"]
            .as_f64()
            .is_some_and(|v| v.is_finite() && (0.0..=120.0).contains(&v))
    {
        return Err("invalid follow_up_secs".into());
    }
    for (key, min, max) in [
        ("max_tokens", 1024, 32768),
        ("max_reply_tokens", 16, 1024),
        ("turn_timeout_ms", 1, 120000),
    ] {
        if !params[key].is_null()
            && !params[key]
                .as_u64()
                .is_some_and(|v| (min..=max).contains(&v))
        {
            return Err(format!("invalid {key}"));
        }
    }
    let assistant = assistant_config(&common)?;
    if !assistant.publish_room
        || !(1024..=32768).contains(&assistant.lm.max_tokens)
        || !(16..=1024).contains(&assistant.max_reply_tokens)
        || assistant.name.is_empty()
        || assistant.name.chars().count() > 256
    {
        return Err("RAG requires room publication and bounded model settings".into());
    }
    let timeout_ms = params["turn_timeout_ms"].as_u64().unwrap_or(15000);
    let member = s("member_id", &assistant.name);
    let collection = s("collection_id", "room-docs");
    if !(1..=120000).contains(&timeout_ms)
        || [&member, &collection]
            .iter()
            .any(|s| s.is_empty() || s.chars().count() > 256)
    {
        return Err("invalid RAG deadline or identity".into());
    }
    let source_probe = if params["_source_probe"].is_null() {
        None
    } else {
        let probe = &params["_source_probe"];
        let nonce = probe["nonce"]
            .as_str()
            .ok_or("invalid source probe nonce")?;
        if !server || nonce.len() != 32 || !nonce.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("source probe requires server mode and a 32-character nonce".into());
        }
        let version = probe["evidence"][0]["source"]["index_version"]
            .as_str()
            .unwrap_or("");
        let hits = protocol::evidence(&probe["evidence"], version)
            .map_err(|_| "invalid source probe evidence")?;
        if hits.len() != 1 || hits[0].source.source_id != "S1" {
            return Err("source probe requires exactly one S1 reference".into());
        }
        Some((nonce.to_owned(), hits))
    };
    Ok(Config {
        assistant,
        server,
        policy,
        url,
        token,
        member,
        collection,
        timeout_ms,
        source_probe,
    })
}

pub struct RagAssistantNode {
    config: Config,
    http: client::Client,
    model: Option<model::Model>,
    room: Mutex<Room>,
    turn: futures_util::lock::Mutex<()>,
    instance: String,
}

/// Dropping the host call cancels its HTTP/model sub-work even with an older host
/// that supplies a non-firing token. No destructor waits for remote work.
struct Work(CancelSource);
impl Drop for Work {
    fn drop(&mut self) {
        self.0
            .cancel(Cancellation::new(CancelReason::CallerCancelled));
    }
}

impl RagAssistantNode {
    pub fn start(config: Config) -> Result<Self, String> {
        let http = client::Client::start(&config)?;
        let model = if config.server {
            None
        } else {
            Some(model::Model::start(config.assistant.lm.clone())?)
        };
        Ok(Self {
            config,
            http,
            model,
            room: Mutex::new(Room::default()),
            turn: futures_util::lock::Mutex::new(()),
            instance: uuid::Uuid::new_v4().to_string(),
        })
    }

    async fn reply<F>(
        &self,
        speaker: &str,
        question: &str,
        seq: Option<u64>,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let _turn = self.turn.lock().await;
        let started = Instant::now();
        let work = Work(ctx.cancel_token().child());
        let cancel = work.0.token();
        if cancel.is_cancelled() {
            return Ok(0);
        }
        let (reply, history) = {
            let mut room = self.room.lock().unwrap();
            room.replies += 1;
            room.pending.clear();
            let mut history: Vec<Value> = room.attributed.iter().cloned().collect();
            history.pop(); // The live question has its own immutable request field.
            (format!("{}-{}", self.instance, room.replies), history)
        };
        let trace = crate::stage_trace::Stage::new(
            "turn",
            &reply,
            json!({
                "session_id":ctx.session_id(),"instance_id":self.instance,"line_seq":seq,
                "backend":if self.config.server {"server"} else {"phone"},"policy":self.config.policy,
            }),
        );
        callback(RuntimeData::Json(presence_thinking(&reply, speaker, seq)))?;
        let mut partial = Partial {
            room: &self.room,
            name: &self.config.assistant.name,
            text: String::new(),
            ended: false,
            cancel: ctx.cancel_token(),
        };
        let mut outputs = 1;
        let mut first_text = true;
        let mut publish = |value: Value| -> Result<(), Error> {
            if cancel.is_cancelled()
                || self.room.lock().unwrap().stopped.as_deref() == Some(reply.as_str())
            {
                return Ok(());
            }
            callback(RuntimeData::Json(value.clone()))?;
            if let Some(text) = value["text"].as_str() {
                if first_text {
                    trace.mark("first_text", json!({}));
                    first_text = false;
                }
                if !partial.text.is_empty() {
                    partial.text.push(' ');
                }
                partial.text.push_str(text);
            }
            outputs += 1;
            Ok(())
        };
        let outcome = if let Some((nonce, hits)) = &self.config.source_probe {
            if question == nonce {
                let line = protocol::sentence(
                    &format!("Source metadata check {nonce} [S1]."),
                    hits,
                    &reply,
                )
                .map_err(|_| Error::InvalidData("invalid source probe".into()))?;
                publish(line)?;
                "answered".to_owned()
            } else {
                "insufficient_evidence".to_owned()
            }
        } else if let Some(text) = conversation_reply(
            question,
            speaker,
            &history,
            &self.config.assistant.wake_words,
        ) {
            publish(sentence_line(&text, &reply))?;
            "conversation".to_owned()
        } else if question.chars().count() > 8000 {
            "invalid_output".to_owned()
        } else {
            let mut request = json!({"version":1, "request_id":uuid::Uuid::new_v4().to_string(), "instance_id":self.instance,
                "session_id":ctx.session_id(), "member_id":self.config.member, "reply_id":reply, "line_seq":seq.unwrap_or(0),
                "question":question, "history":history, "retrieval_policy":self.config.policy, "collection_id":self.config.collection,
                "deadline_ms":self.config.timeout_ms.saturating_sub(started.elapsed().as_millis() as u64).max(1)});
            while serde_json::to_vec(&request)
                .map_err(|e| Error::InvalidData(e.to_string()))?
                .len()
                > 128 * 1024
            {
                let history = request["history"].as_array_mut().unwrap();
                if history.is_empty() {
                    return Err(Error::InvalidData(
                        "RAG request exceeds protocol limit".into(),
                    ));
                }
                history.remove(0);
            }
            self.answer(request, &reply, started, cancel.clone(), &mut publish)
                .await?
        };
        if cancel.is_cancelled() || outcome == "cancelled" {
            return Ok(outputs);
        }
        if let Some(message) = failure_message(&outcome) {
            publish(sentence_line(message, &reply))?;
        }
        drop(publish);
        partial.ended = true;
        if !partial.text.is_empty() {
            self.room.lock().unwrap().note_attributed(
                &self.config.assistant.name,
                &partial.text,
                true,
                false,
            );
        }
        callback(RuntimeData::Json(presence_idle(&reply)))?;
        trace.finish(json!({"outcome":outcome}));
        eprintln!(
            "[{NODE_TYPE}] reply={reply} backend={} policy={} outcome={outcome} total_ms={}",
            if self.config.server {
                "server"
            } else {
                "phone"
            },
            self.config.policy,
            started.elapsed().as_millis()
        );
        Ok(outputs + 1)
    }

    async fn answer<F>(
        &self,
        request: Value,
        reply: &str,
        started: Instant,
        cancel: remotemedia_plugin_sdk::traits::CancelToken,
        publish: &mut F,
    ) -> Result<String, Error>
    where
        F: FnMut(Value) -> Result<(), Error> + Send,
    {
        let http_trace = crate::stage_trace::Stage::new(
            "http",
            reply,
            json!({
                "request_id":request["request_id"],"generates_answer":self.config.server,
            }),
        );
        let mut remote_timings = serde_json::Map::new();
        let (output, mut events) = tokio::sync::mpsc::channel(16);
        if self
            .http
            .send(client::Job {
                request: request.clone(),
                cancel: cancel.clone(),
                output,
            })
            .is_err()
        {
            return Ok("unavailable".into());
        }
        let mut hits = Vec::new();
        let mut snapshot = None;
        let mut sentences = Sentences::default();
        let mut generated = false;
        let mut terminal = "unavailable".to_owned();
        while let Some(event) = events.recv().await {
            if cancel.is_cancelled() {
                return Ok("cancelled".into());
            }
            let value = match event {
                client::Event::Wire(value) => value,
                client::Event::Failure(outcome) => return Ok(outcome.into()),
            };
            let version = value["index_version"].as_str().unwrap_or("");
            if snapshot.as_deref().is_some_and(|v| v != version) {
                return Ok("invalid_output".into());
            }
            snapshot = Some(version.to_owned());
            match value["kind"].as_str() {
                Some("evidence") if hits.is_empty() && !generated => {
                    hits = match protocol::evidence(&value["evidence"], version) {
                        Ok(hits) => hits,
                        Err(_) => return Ok("invalid_output".into()),
                    };
                }
                Some("delta") if self.config.server && !hits.is_empty() => {
                    let Some(text) = value["text"].as_str() else {
                        return Ok("invalid_output".into());
                    };
                    if !generated && !text.is_empty() {
                        http_trace.mark("first_delta", json!({}));
                    }
                    if !emit_delta(&mut sentences, text, &hits, reply, publish)? {
                        return Ok("invalid_output".into());
                    }
                    generated |= !text.is_empty();
                }
                Some("outcome") => {
                    terminal = value["outcome"]
                        .as_str()
                        .unwrap_or("invalid_output")
                        .to_owned();
                    for key in [
                        "retrieval",
                        "grading",
                        "rewrite",
                        "generation",
                        "generation_first_delta",
                        "total",
                    ] {
                        if let Some(ms) = value["timings_ms"][key]
                            .as_f64()
                            .filter(|v| v.is_finite() && *v >= 0.0)
                        {
                            remote_timings.insert(key.into(), json!(ms));
                        }
                    }
                    if !self.config.server && terminal == "retrieved" {
                        hits = match protocol::evidence(&value["evidence"], version) {
                            Ok(hits) if !hits.is_empty() => hits,
                            _ => return Ok("invalid_output".into()),
                        };
                    }
                    // Wait for HTTP EOF so malformed trailing events cannot look complete.
                }
                _ => return Ok("invalid_output".into()),
            }
        }
        http_trace.finish(json!({"outcome":terminal,"service_timings_ms":remote_timings}));
        if !self.config.server && terminal == "retrieved" {
            return self
                .phone_answer(&request, reply, started, cancel, hits, publish)
                .await;
        }
        if terminal == "answered" && generated {
            if let Some(rest) = sentences.finish() {
                let Ok(line) = protocol::sentence(&rest, &hits, reply) else {
                    return Ok("invalid_output".into());
                };
                publish(line)?;
            }
            return Ok(terminal);
        }
        Ok(if terminal == "answered" {
            "invalid_output".into()
        } else {
            terminal
        })
    }

    async fn phone_answer<F>(
        &self,
        request: &Value,
        reply: &str,
        started: Instant,
        cancel: remotemedia_plugin_sdk::traits::CancelToken,
        hits: Vec<Evidence>,
        publish: &mut F,
    ) -> Result<String, Error>
    where
        F: FnMut(Value) -> Result<(), Error> + Send,
    {
        let trace = crate::stage_trace::Stage::new("phone_generation", reply, json!({}));
        let config = &self.config.assistant;
        let (output, mut events) = tokio::sync::mpsc::channel(16);
        let job = model::Job {
            question: request["question"].as_str().unwrap_or("").into(),
            history: request["history"].as_array().unwrap().clone(),
            evidence: hits,
            reply_id: reply.into(),
            reply_tokens: config.max_reply_tokens,
            deadline: started + Duration::from_millis(self.config.timeout_ms),
            cancel: cancel.clone(),
            output,
        };
        if self.model.as_ref().unwrap().send(job).is_err() {
            return Ok("unavailable".into());
        }
        let mut sentences = Sentences::default();
        let mut generated = false;
        let mut hits = Vec::new();
        while let Some(piece) = events.recv().await {
            if cancel.is_cancelled() {
                return Ok("cancelled".into());
            }
            match piece {
                model::Event::Prepared(selected) => hits = selected,
                model::Event::InsufficientContext => return Ok("insufficient_evidence".into()),
                model::Event::Piece(Piece::Text(text)) => {
                    if !generated && !text.is_empty() {
                        trace.mark("first_delta", json!({}));
                    }
                    if !emit_delta(&mut sentences, &text, &hits, reply, publish)? {
                        return Ok("invalid_output".into());
                    }
                    generated |= !text.is_empty();
                }
                model::Event::Piece(Piece::Error(error)) => {
                    return Ok(if error == "timeout" {
                        "timeout"
                    } else {
                        "unavailable"
                    }
                    .into())
                }
                model::Event::Piece(Piece::Done) => {
                    if let Some(rest) = sentences.finish() {
                        if rest == "INSUFFICIENT_EVIDENCE" {
                            trace.finish(json!({"outcome":"insufficient_evidence"}));
                            return Ok("insufficient_evidence".into());
                        }
                        let Ok(value) = protocol::sentence(&rest, &hits, reply) else {
                            return Ok("invalid_output".into());
                        };
                        publish(value)?;
                    }
                    let outcome = if generated {
                        "answered"
                    } else {
                        "invalid_output"
                    };
                    trace.finish(json!({"outcome":outcome}));
                    return Ok(outcome.into());
                }
            }
        }
        Ok("unavailable".into())
    }
}

fn emit_delta<F>(
    sentences: &mut Sentences,
    text: &str,
    hits: &[Evidence],
    reply: &str,
    publish: &mut F,
) -> Result<bool, Error>
where
    F: FnMut(Value) -> Result<(), Error>,
{
    let Ok(ready) = sentences.push(text) else {
        return Ok(false);
    };
    for sentence in ready {
        let Ok(value) = protocol::sentence(&sentence, hits, reply) else {
            return Ok(false);
        };
        publish(value)?;
    }
    Ok(true)
}

fn failure_message(outcome: &str) -> Option<&'static str> {
    match outcome {
        "answered" | "conversation" | "cancelled" => None,
        "insufficient_evidence" => {
            Some("I couldn't find enough information in the documents to answer that.")
        }
        "timeout" => Some("The document lookup or answer took too long. Please try again."),
        "invalid_output" => Some("I couldn't substantiate that answer with the retrieved sources."),
        _ => Some("The document service is unavailable. Please try again."),
    }
}

/// A deliberately narrow history-only path; ambiguous questions always retrieve.
fn conversation_reply(
    question: &str,
    speaker: &str,
    history: &[Value],
    names: &[String],
) -> Option<String> {
    let words: Vec<String> = question
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|s| !s.is_empty())
        .map(str::to_lowercase)
        .filter(|s| !names.contains(s) && !["hey", "hi", "hello"].contains(&s.as_str()))
        .collect();
    match words.join(" ").as_str() {
        "how are you" => Some("I'm here and ready to help with the documents.".into()),
        "what did i just say" | "what did i say earlier" | "what was i saying" => Some(
            history
                .iter()
                .rev()
                .find(|v| v["role"] == "person" && v["author"] == speaker)
                .and_then(|v| v["text"].as_str())
                .map(|text| format!("You said: {}", text.chars().take(500).collect::<String>()))
                .unwrap_or_else(|| {
                    "I don't have an earlier line from you in this room's history.".into()
                }),
        ),
        _ => None,
    }
}

#[async_trait]
impl AsyncStreamingNode for RagAssistantNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }
    fn generator(&self) -> Option<GeneratorRole<'_>> {
        GeneratorRole::text(self)
    }
    async fn process(&self, _: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::InvalidData(
            "RagAssistantNode streams replies".into(),
        ))
    }
    async fn process_streaming<F>(
        &self,
        _: RuntimeData,
        _: Option<String>,
        _: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        Ok(0)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let (text, seq, history, agent) = match room_frame(&data) {
            Some(RoomFrame::Line {
                text,
                seq,
                history,
                agent,
            }) => (text, seq, history, agent),
            Some(RoomFrame::Stop(reply)) => {
                self.room.lock().unwrap().stopped = Some(reply);
                return Ok(0);
            }
            _ => return Ok(0),
        };
        let speaker = room_speaker_name(ctx);
        if agent {
            let own = speaker.eq_ignore_ascii_case(&self.config.assistant.name)
                || ctx
                    .origin()
                    .is_some_and(|o| o.id().eq_ignore_ascii_case(&self.config.member));
            let author = if own {
                &self.config.assistant.name
            } else {
                &speaker
            };
            let interrupted = matches!(&data, RuntimeData::Json(v) if v["interrupted"] == true);
            self.room
                .lock()
                .unwrap()
                .note_attributed(author, &text, true, interrupted);
            return Ok(0);
        }
        let common = &self.config.assistant;
        let addressed = addressed(&common.wake_words, common.respond_all, &text);
        if history {
            self.room
                .lock()
                .unwrap()
                .recall(&speaker, &text, &addressed, Instant::now());
            return Ok(0);
        }
        let said = self.room.lock().unwrap().hear(
            &speaker,
            &text,
            &addressed,
            common.follow_up,
            Instant::now(),
        );
        match said {
            Some(said) => self.reply(&speaker, &said, seq, ctx, callback).await,
            None if matches!(addressed, Addressed::NameOnly) => {
                callback(RuntimeData::Json(presence_listening(&speaker)))?;
                Ok(1)
            }
            None => Ok(0),
        }
    }
}

#[async_trait]
impl Cancellable for RagAssistantNode {
    fn cancel_strategy(&self) -> CancelStrategy {
        CancelStrategy::ObservesToken
    }
    async fn on_cancel(&self, _: &CallContext, cancellation: &Cancellation) {
        if cancellation.disposition.pending_input == Retention::Discard {
            self.room.lock().unwrap().pending.clear();
        }
    }
}
impl TextGenerator for RagAssistantNode {}

#[derive(Default)]
pub struct RagAssistantFactory;
impl FfiNodeFactory for RagAssistantFactory {
    fn node_type(&self) -> RString {
        NODE_TYPE.into()
    }
    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap_or(Value::Null);
        match config_from_params(&params).and_then(RagAssistantNode::start) {
            Ok(node) => ROk(FfiNode_TO::from_value(
                StreamingNodeFfiAdapter::new(node),
                TD_Opaque,
            )),
            Err(error) => RErr(format!("{NODE_TYPE}: {error}").into()),
        }
    }
}

#[cfg(test)]
mod tests;
