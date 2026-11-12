//! LiteRT-LM's C API (`c/engine.h`, `c/conversation.h`, API 1.x), resolved
//! at run time: an engine for a `.litertlm` language model (e.g. Gemma 4
//! E2B) and conversations that stream replies.
//!
//! The library is LiteRT-LM's C API build: `liblitert-lm.so` on Android,
//! `litert-lm.dll` on Windows (both in `litert_lm_c_api-*.zip` on the
//! LiteRT-LM releases page). Android's loader rejects linking against the
//! `.so` (it has no SONAME), so it is always opened with `dlopen`.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use libloading::Library;

type H = *mut c_void;
type Status = c_int;
type StreamCallback = unsafe extern "C" fn(*mut c_void, *const c_void);

const LOG_ERROR: c_int = 4;

struct Api {
    /// Never unloaded: see [`crate::litert::keep_loaded`].
    _lib: std::mem::ManuallyDrop<Library>,
    source: String,
    last_error: unsafe extern "C" fn() -> *const c_char,
    set_min_log_level: unsafe extern "C" fn(c_int) -> Status,
    settings_create:
        unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, *const c_char, *mut H) -> Status,
    settings_delete: unsafe extern "C" fn(H),
    settings_max_tokens: unsafe extern "C" fn(H, c_int) -> Status,
    settings_threads: unsafe extern "C" fn(H, c_int) -> Status,
    settings_cache_dir: unsafe extern "C" fn(H, *const c_char) -> Status,
    engine_create: unsafe extern "C" fn(H, *mut H) -> Status,
    engine_delete: unsafe extern "C" fn(H),
    config_create: unsafe extern "C" fn(*mut H) -> Status,
    config_delete: unsafe extern "C" fn(H),
    config_system: unsafe extern "C" fn(H, *const c_char) -> Status,
    conversation_create: unsafe extern "C" fn(H, H, *mut H) -> Status,
    conversation_delete: unsafe extern "C" fn(H),
    conversation_tokens: unsafe extern "C" fn(H, *mut usize) -> Status,
    conversation_cancel: unsafe extern "C" fn(H) -> Status,
    send_stream: unsafe extern "C" fn(H, *const c_char, *const c_char, H, StreamCallback, *mut c_void) -> Status,
    args_create: unsafe extern "C" fn(*mut H) -> Status,
    args_delete: unsafe extern "C" fn(H),
    args_max_output: unsafe extern "C" fn(H, c_int) -> Status,
    chunk_text: unsafe extern "C" fn(*const c_void, *mut *const c_char) -> Status,
    chunk_final: unsafe extern "C" fn(*const c_void, *mut bool) -> Status,
    chunk_error: unsafe extern "C" fn(*const c_void, *mut *const c_char) -> Status,
}

macro_rules! sym {
    ($lib:expr, $name:literal) => {
        *$lib
            .get(concat!($name, "\0").as_bytes())
            .map_err(|e| format!("{}: {e}", $name))?
    };
}

fn c_string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
    }
}

impl Api {
    fn load(path: Option<&str>) -> Result<Self, String> {
        let candidates: Vec<String> = match path
            .map(str::to_string)
            .or_else(|| std::env::var("REMOTEMEDIA_LITERT_LM_LIB").ok())
        {
            Some(p) => vec![p],
            None => ["liblitert-lm.so", "litert-lm.dll", "liblitert-lm.dylib"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        };
        let mut errors = Vec::new();
        for name in &candidates {
            match unsafe { Library::new(name) } {
                Ok(lib) => match unsafe { Self::resolve(lib, name) } {
                    Ok(api) => return Ok(api),
                    Err(e) => errors.push(format!("{name}: missing {e}")),
                },
                Err(e) => errors.push(format!("{name}: {e}")),
            }
        }
        Err(format!("no LiteRT-LM C API library: {}", errors.join("; ")))
    }

    unsafe fn resolve(lib: Library, source: &str) -> Result<Self, String> {
        Ok(Self {
            last_error: sym!(lib, "litert_lm_get_last_error_message"),
            set_min_log_level: sym!(lib, "litert_lm_set_min_log_level"),
            settings_create: sym!(lib, "litert_lm_engine_settings_create"),
            settings_delete: sym!(lib, "litert_lm_engine_settings_delete"),
            settings_max_tokens: sym!(lib, "litert_lm_engine_settings_set_max_num_tokens"),
            settings_threads: sym!(lib, "litert_lm_engine_settings_set_num_threads"),
            settings_cache_dir: sym!(lib, "litert_lm_engine_settings_set_cache_dir"),
            engine_create: sym!(lib, "litert_lm_engine_create"),
            engine_delete: sym!(lib, "litert_lm_engine_delete"),
            config_create: sym!(lib, "litert_lm_conversation_config_create"),
            config_delete: sym!(lib, "litert_lm_conversation_config_delete"),
            config_system: sym!(lib, "litert_lm_conversation_config_set_system_message"),
            conversation_create: sym!(lib, "litert_lm_conversation_create"),
            conversation_delete: sym!(lib, "litert_lm_conversation_delete"),
            conversation_tokens: sym!(lib, "litert_lm_conversation_get_token_count"),
            conversation_cancel: sym!(lib, "litert_lm_conversation_cancel_process"),
            send_stream: sym!(lib, "litert_lm_conversation_send_message_stream"),
            args_create: sym!(lib, "litert_lm_conversation_optional_args_create"),
            args_delete: sym!(lib, "litert_lm_conversation_optional_args_delete"),
            args_max_output: sym!(lib, "litert_lm_conversation_optional_args_set_max_output_tokens"),
            chunk_text: sym!(lib, "litert_lm_stream_chunk_get_text"),
            chunk_final: sym!(lib, "litert_lm_stream_chunk_is_final"),
            chunk_error: sym!(lib, "litert_lm_stream_chunk_get_error"),
            source: source.to_string(),
            _lib: crate::litert::keep_loaded(lib),
        })
    }

    fn check(&self, status: Status, what: &str) -> Result<(), String> {
        if status == 0 {
            return Ok(());
        }
        let detail = c_string(unsafe { (self.last_error)() });
        Err(format!("{what}: status {status} {detail}"))
    }
}

#[derive(Clone, Debug)]
pub struct LmConfig {
    pub model_path: String,
    /// The C API library; `None` tries the platform's usual names.
    pub runtime_lib: Option<String>,
    /// `gpu` or `cpu`.
    pub backend: String,
    /// Context length (prompt and reply) per conversation.
    pub max_tokens: i32,
    /// CPU threads (`cpu` backend); 0 leaves the default.
    pub threads: i32,
    /// Where compiled GPU kernels are cached between runs.
    pub cache_dir: Option<String>,
}

impl LmConfig {
    pub fn new(model_path: impl Into<String>) -> Self {
        Self {
            model_path: model_path.into(),
            runtime_lib: None,
            backend: "gpu".into(),
            max_tokens: 4096,
            threads: 0,
            cache_dir: None,
        }
    }
}

pub struct Engine {
    api: Arc<Api>,
    engine: H,
    settings: H,
    pub max_tokens: usize,
    pub runtime: String,
}

unsafe impl Send for Engine {}

/// One piece of a streamed reply.
pub enum Piece {
    Text(String),
    Error(String),
    Done,
}

struct StreamState {
    tx: mpsc::Sender<Piece>,
    api: Arc<Api>,
}

unsafe extern "C" fn on_chunk(data: *mut c_void, chunk: *const c_void) {
    let state = &*(data as *const StreamState);
    let api = &state.api;
    let mut text: *const c_char = std::ptr::null();
    let mut error: *const c_char = std::ptr::null();
    let mut done = false;
    (api.chunk_text)(chunk, &mut text);
    (api.chunk_error)(chunk, &mut error);
    (api.chunk_final)(chunk, &mut done);
    if !text.is_null() {
        let raw = c_string(text);
        let _ = state.tx.send(Piece::Text(chunk_text(&raw)));
    }
    if !error.is_null() {
        let _ = state.tx.send(Piece::Error(c_string(error)));
    }
    if done {
        let _ = state.tx.send(Piece::Done);
    }
}

/// A chunk is a message (`{"role":"assistant","content":[{"type":"text","text":"..."}]}`)
/// holding the new text; anything else is taken as text.
fn chunk_text(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => match &v["content"] {
            serde_json::Value::Array(parts) => parts
                .iter()
                .filter_map(|p| p["text"].as_str())
                .collect(),
            serde_json::Value::String(s) => s.clone(),
            _ => String::new(),
        },
        Err(_) => raw.to_string(),
    }
}

impl Engine {
    pub fn load(config: &LmConfig) -> Result<Self, String> {
        let api = Arc::new(Api::load(config.runtime_lib.as_deref())?);
        unsafe {
            (api.set_min_log_level)(LOG_ERROR);
            let model = CString::new(config.model_path.as_str()).map_err(|e| e.to_string())?;
            let backend = CString::new(config.backend.as_str()).map_err(|e| e.to_string())?;
            let mut settings: H = std::ptr::null_mut();
            api.check(
                (api.settings_create)(
                    model.as_ptr(),
                    backend.as_ptr(),
                    std::ptr::null(),
                    std::ptr::null(),
                    &mut settings,
                ),
                "engine settings",
            )?;
            let mut this = Self {
                api: Arc::clone(&api),
                engine: std::ptr::null_mut(),
                settings,
                max_tokens: config.max_tokens as usize,
                runtime: String::new(),
            };
            api.check(
                (api.settings_max_tokens)(settings, config.max_tokens),
                "max tokens",
            )?;
            if config.threads > 0 {
                api.check((api.settings_threads)(settings, config.threads), "threads")?;
            }
            if let Some(dir) = &config.cache_dir {
                let _ = std::fs::create_dir_all(dir);
                let dir = CString::new(dir.as_str()).map_err(|e| e.to_string())?;
                api.check((api.settings_cache_dir)(settings, dir.as_ptr()), "cache dir")?;
            }
            let start = Instant::now();
            api.check((api.engine_create)(settings, &mut this.engine), "engine")?;
            this.runtime = format!(
                "{} ({}, loaded in {:.1} s)",
                api.source,
                config.backend,
                start.elapsed().as_secs_f64()
            );
            Ok(this)
        }
    }

    /// A conversation with `system` as its system message.
    pub fn conversation(&self, system: &str) -> Result<Conversation, String> {
        let api = &self.api;
        unsafe {
            let mut config: H = std::ptr::null_mut();
            api.check((api.config_create)(&mut config), "conversation config")?;
            let message = serde_json::json!({ "type": "text", "text": system }).to_string();
            let message = CString::new(message).map_err(|e| e.to_string())?;
            let set = (api.config_system)(config, message.as_ptr());
            let mut conversation: H = std::ptr::null_mut();
            let created = (api.conversation_create)(self.engine, config, &mut conversation);
            (api.config_delete)(config);
            api.check(set, "system message")?;
            api.check(created, "conversation")?;
            Ok(Conversation {
                api: Arc::clone(api),
                handle: conversation,
                state: None,
            })
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        unsafe {
            if !self.engine.is_null() {
                (self.api.engine_delete)(self.engine);
            }
            (self.api.settings_delete)(self.settings);
        }
    }
}

pub struct Conversation {
    api: Arc<Api>,
    handle: H,
    /// The callback's state, alive until the conversation is deleted (a
    /// callback may still run after a cancel).
    state: Option<Box<StreamState>>,
}

unsafe impl Send for Conversation {}

impl Conversation {
    /// Sends a user message; the reply arrives as pieces on the returned
    /// channel, ending with `Piece::Done` (or `Piece::Error`).
    pub fn send(&mut self, text: &str, max_output_tokens: i32) -> Result<mpsc::Receiver<Piece>, String> {
        let (tx, rx) = mpsc::channel();
        let message = serde_json::json!({
            "role": "user",
            "content": [{ "type": "text", "text": text }],
        })
        .to_string();
        let message = CString::new(message).map_err(|e| e.to_string())?;
        let state = Box::new(StreamState {
            tx,
            api: Arc::clone(&self.api),
        });
        let data = &*state as *const StreamState as *mut c_void;
        // The previous reply's state can go: that stream has finished.
        self.state = Some(state);
        let api = &self.api;
        unsafe {
            let mut args: H = std::ptr::null_mut();
            api.check((api.args_create)(&mut args), "optional args")?;
            if max_output_tokens > 0 {
                (api.args_max_output)(args, max_output_tokens);
            }
            let status = (api.send_stream)(
                self.handle,
                message.as_ptr(),
                std::ptr::null(),
                args,
                on_chunk,
                data,
            );
            (api.args_delete)(args);
            api.check(status, "send message")?;
        }
        Ok(rx)
    }

    /// Tokens in the conversation's KV cache.
    pub fn tokens(&self) -> usize {
        let mut n = 0usize;
        unsafe { (self.api.conversation_tokens)(self.handle, &mut n) };
        n
    }

    /// Stops the reply in progress. The conversation cannot be used again.
    pub fn cancel(&self) {
        unsafe { (self.api.conversation_cancel)(self.handle) };
    }
}

impl Drop for Conversation {
    fn drop(&mut self) {
        unsafe { (self.api.conversation_delete)(self.handle) };
    }
}

/// Splits streamed text into sentences as they complete: a sentence ends at
/// `.`, `!`, `?` (or `:`/`;` once it is long) followed by whitespace, or at a
/// newline.
#[derive(Default)]
pub struct Sentences {
    pending: String,
    /// Let the first piece end at a comma once it is this long (0: never).
    first_clause: usize,
    emitted: bool,
}

impl Sentences {
    /// A splitter whose first piece may end at a comma (after 24
    /// characters), so speech can start before the first sentence is done.
    pub fn first_clause() -> Self {
        Self {
            first_clause: 24,
            ..Self::default()
        }
    }

    pub fn push(&mut self, text: &str) -> Vec<String> {
        self.pending.push_str(text);
        let mut out = Vec::new();
        loop {
            let bytes = self.pending.as_bytes();
            let mut cut = None;
            for i in 0..bytes.len() {
                let c = bytes[i];
                let next_space = bytes.get(i + 1).map_or(false, |b| b.is_ascii_whitespace());
                let long = i > 60;
                let early_clause = !self.emitted && self.first_clause > 0 && i >= self.first_clause;
                if c == b'\n'
                    || (matches!(c, b'.' | b'!' | b'?') && next_space)
                    || (long && matches!(c, b':' | b';') && next_space)
                    || (early_clause && c == b',' && next_space)
                {
                    // Not after a single capital ("Mr.", "U.S.") or a digit ("3.").
                    if c == b'.' && i >= 1 {
                        let before = &self.pending[..i];
                        let word = before.rsplit(|ch: char| ch.is_whitespace()).next().unwrap_or("");
                        if word.len() <= 2 && word.chars().next().map_or(false, |ch| ch.is_uppercase()) {
                            continue;
                        }
                    }
                    cut = Some(i + 1);
                    break;
                }
            }
            let Some(at) = cut else { break };
            let sentence = self.pending[..at].trim().to_string();
            self.pending = self.pending[at..].to_string();
            if !sentence.is_empty() {
                self.emitted = true;
                out.push(sentence);
            }
        }
        out
    }

    /// What is left at the end of the reply.
    pub fn finish(&mut self) -> Option<String> {
        let rest = std::mem::take(&mut self.pending).trim().to_string();
        (!rest.is_empty()).then_some(rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentences_split_as_they_complete() {
        let mut s = Sentences::default();
        assert!(s.push("Hello there").is_empty());
        assert_eq!(s.push(". How are"), vec!["Hello there."]);
        assert_eq!(s.push(" you? Dr. Smith said 3.5 is"), vec!["How are you?"]);
        assert!(s.push(" fine").is_empty());
        assert_eq!(s.finish().as_deref(), Some("Dr. Smith said 3.5 is fine"));
    }

    #[test]
    fn first_piece_may_end_at_a_comma() {
        let mut s = Sentences::first_clause();
        assert_eq!(
            s.push("The tallest mountain on Earth, by height above sea level, is Everest. "),
            vec!["The tallest mountain on Earth,", "by height above sea level, is Everest."]
        );
    }

    #[test]
    fn chunk_text_reads_messages() {
        assert_eq!(
            chunk_text(r#"{"role":"assistant","content":[{"type":"text","text":" pixel"}]}"#),
            " pixel"
        );
        assert_eq!(chunk_text("plain"), "plain");
    }
}
