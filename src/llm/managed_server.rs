//! Executor-owned llama-server instances shared across pipeline sessions.
//! The selected INI section is translated explicitly; unsupported settings fail closed.
use crate::manifest::Manifest;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedLlamaServerConfig {
    pub executable: PathBuf,
    pub models_ini: PathBuf,
    pub preset: String,
    pub gpu: String,
    /// Explicit deployment override; never inferred from available memory.
    #[serde(default)]
    pub context_size: Option<u32>,
    #[serde(default = "startup_timeout")]
    pub startup_timeout_secs: u64,
}
fn startup_timeout() -> u64 {
    300
}
fn failure(message: impl Into<String>) -> Error {
    Error::Execution(message.into())
}

/// Only entries in the named section are active; comments and other presets cannot
/// enable speculative decoding or override the requested model.
fn parse_preset(source: &str, preset: &str) -> Result<BTreeMap<String, String>> {
    let mut selected = false;
    let mut found = false;
    let mut values = BTreeMap::new();
    for (index, line) in source.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with([';', '#']) {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            selected = &line[1..line.len() - 1] == preset;
            if selected && found {
                return Err(failure("duplicate model preset section"));
            }
            found |= selected;
        } else if selected {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| failure(format!("invalid preset entry at line {}", index + 1)))?;
            let (key, value) = (key.trim(), value.trim());
            if value.is_empty() || values.insert(key.into(), value.into()).is_some() {
                return Err(failure(format!("empty or duplicate preset setting: {key}")));
            }
        }
    }
    if !found {
        return Err(failure(format!("model preset not found: {preset}")));
    }
    if !values.contains_key("model") {
        return Err(failure("model preset requires model"));
    }
    Ok(values)
}

fn preset_args(values: &BTreeMap<String, String>) -> Result<Vec<String>> {
    let mut args = Vec::new();
    for (key, value) in values {
        let flag = match key.as_str() {
            "c" => "ctx-size",
            "n-gpu-layers"
            | "model"
            | "mmproj"
            | "cache-ram"
            | "threads"
            | "batch-size"
            | "ubatch-size"
            | "reasoning-budget"
            | "parallel"
            | "temp"
            | "top-p"
            | "top-k"
            | "min-p"
            | "repeat-penalty"
            | "presence-penalty"
            | "chat-template-kwargs"
            | "image-min-tokens"
            | "image-max-tokens" => key,
            "fa" => "flash-attn",
            "no-context-shift" | "no-mmproj-offload" | "jinja" | "kv-unified" | "cont-batching" => {
                let enabled = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(failure(format!("{key} requires true or false"))),
                };
                let flag = if enabled {
                    key.clone()
                } else if let Some(positive) = key.strip_prefix("no-") {
                    positive.to_string()
                } else {
                    format!("no-{key}")
                };
                args.push(format!("--{flag}"));
                continue;
            }
            _ => {
                return Err(failure(format!(
                    "unsupported managed llama-server setting: {key}"
                )))
            }
        };
        if key == "chat-template-kwargs" {
            let kwargs: serde_json::Value =
                serde_json::from_str(value).map_err(|e| failure(e.to_string()))?;
            if !kwargs.is_object() {
                return Err(failure("chat-template-kwargs must be an object"));
            }
        }
        args.extend([format!("--{flag}"), value.clone()]);
    }
    Ok(args)
}

struct ManagedServer {
    child: Child,
    base_url: String,
    api_key: String,
    source: BTreeMap<String, String>,
}
impl ManagedServer {
    async fn start(
        config: &ManagedLlamaServerConfig,
        source: BTreeMap<String, String>,
    ) -> Result<Self> {
        let args = preset_args(&source)?;
        for key in ["model", "mmproj"] {
            if let Some(path) = source.get(key) {
                if !std::path::Path::new(path).is_file() {
                    return Err(failure(format!("{key} file missing: {path}")));
                }
            }
        }
        let help = tokio::time::timeout(
            Duration::from_secs(15),
            Command::new(&config.executable)
                .arg("--help")
                .env("CUDA_VISIBLE_DEVICES", &config.gpu)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| failure("llama-server --help timed out"))?
        .map_err(|e| failure(format!("cannot inspect llama-server: {e}")))?;
        if !help.status.success() {
            return Err(failure("llama-server --help failed"));
        }
        let help_text = String::from_utf8_lossy(&help.stdout);
        for arg in args.iter().filter(|s| s.starts_with("--")) {
            if !help_text
                .split(|c: char| c.is_whitespace() || c == ',')
                .any(|word| word == arg)
            {
                return Err(failure(format!("llama-server does not support {arg}")));
            }
        }
        // Bind only loopback. A bind race fails startup rather than adopting an
        // unrelated service: readiness requires this child's per-execution key.
        let socket =
            std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| failure(e.to_string()))?;
        let port = socket
            .local_addr()
            .map_err(|e| failure(e.to_string()))?
            .port();
        let api_key = uuid::Uuid::new_v4().to_string();
        drop(socket);
        let child = Command::new(&config.executable)
            .args(&args)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--alias",
                &config.preset,
            ])
            .env("CUDA_VISIBLE_DEVICES", &config.gpu)
            .env("LLAMA_API_KEY", &api_key)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| failure(format!("cannot start llama-server: {e}")))?;
        let mut server = Self {
            child,
            base_url: format!("http://127.0.0.1:{port}/v1"),
            api_key,
            source,
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|e| failure(e.to_string()))?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(config.startup_timeout_secs);
        loop {
            if let Some(status) = server
                .child
                .try_wait()
                .map_err(|e| failure(e.to_string()))?
            {
                return Err(failure(format!(
                    "managed llama-server exited during startup: {status}; see server diagnostics"
                )));
            }
            if let Ok(response) = client
                .get(format!("{}/models", server.base_url))
                .bearer_auth(&server.api_key)
                .send()
                .await
            {
                if response.status().is_success() {
                    return Ok(server);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure("managed llama-server readiness timed out"));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Strong ownership belongs to the executor, not to a browser or an individual
/// session. Dropping the executor kills its children, including failed startups.
#[derive(Default)]
pub struct ManagedLlamaServers {
    servers: Mutex<BTreeMap<String, ManagedServer>>,
}
impl ManagedLlamaServers {
    pub async fn prepare(&self, manifest: &Manifest) -> Result<Manifest> {
        let mut prepared = manifest.clone();
        for node in &mut prepared.nodes {
            let Some(raw) = node.params.get("managed_server") else {
                continue;
            };
            if !matches!(
                node.node_type.as_str(),
                "OpenAIChatNode" | "MultimodalLLMNode"
            ) {
                return Err(failure(
                    "managed_server requires OpenAIChatNode or MultimodalLLMNode",
                ));
            }
            let config: ManagedLlamaServerConfig =
                serde_json::from_value(raw.clone()).map_err(|e| failure(e.to_string()))?;
            if node
                .params
                .get("provider")
                .and_then(|v| v.as_str())
                .is_some_and(|p| p != "open_ai")
            {
                return Err(failure("managed llama-server requires the OpenAI provider"));
            }
            let source_text = tokio::fs::read_to_string(&config.models_ini)
                .await
                .map_err(|e| failure(format!("cannot read models INI: {e}")))?;
            let mut source = parse_preset(&source_text, &config.preset)?;
            if let Some(context) = config.context_size {
                if context == 0 { return Err(failure("context_size override must be positive")); }
                source.insert("c".into(), context.to_string());
            }
            let key = serde_json::to_string(&config).map_err(|e| failure(e.to_string()))?;
            let mut servers = self.servers.lock().await;
            if let Some(server) = servers.get_mut(&key) {
                if source != server.source {
                    return Err(failure(
                        "model preset changed; restart the executor to apply it",
                    ));
                }
                if let Some(status) = server
                    .child
                    .try_wait()
                    .map_err(|e| failure(e.to_string()))?
                {
                    return Err(failure(format!("managed llama-server died: {status}")));
                }
            } else {
                servers.insert(key.clone(), ManagedServer::start(&config, source).await?);
            }
            let server = &servers[&key];
            node.params["base_url"] = server.base_url.clone().into();
            node.params["api_key"] = server.api_key.clone().into();
            node.params["model"] = config.preset.into();
            node.params
                .as_object_mut()
                .unwrap()
                .remove("managed_server");
        }
        Ok(prepared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_preset_preserves_flags_and_ignores_commented_drafts() {
        let source = "model=global\n[other]\nmodel=wrong\n[Bonsai]\nmodel=/models/b.gguf\nmmproj=/models/p.gguf\nc=262144\nkv-unified=false\nno-context-shift=true\nno-mmproj-offload=true\njinja=true\ncont-batching=true\nfa=on\nreasoning-budget=131072\nchat-template-kwargs={\"preserve_thinking\":true}\n; model-draft=disabled\n";
        let parsed = parse_preset(source, "Bonsai").unwrap();
        let args = preset_args(&parsed).unwrap();
        assert!(args.windows(2).any(|a| a == ["--ctx-size", "262144"]));
        assert!(args
            .windows(2)
            .any(|a| a == ["--reasoning-budget", "131072"]));
        for flag in [
            "--no-kv-unified",
            "--no-context-shift",
            "--no-mmproj-offload",
            "--jinja",
            "--cont-batching",
        ] {
            assert!(args.contains(&flag.to_string()));
        }
        assert!(!args.iter().any(|a| a.contains("draft") || a == "wrong"));
    }
    #[test]
    fn invalid_presets_fail_instead_of_silently_falling_back() {
        for input in [
            "[x]\nmodel=a\nmodel=b",
            "[x]\nmodel=a\n[x]\nmodel=b",
            "[x]\nc=10",
        ] {
            assert!(parse_preset(input, "x").is_err());
        }
        assert!(parse_preset("[other]\nmodel=a", "x").is_err());
        for input in ["[x]\nmodel=a\nunknown=1", "[x]\nmodel=a\njinja=maybe"] {
            assert!(preset_args(&parse_preset(input, "x").unwrap()).is_err());
        }
    }
}

#[cfg(all(test, unix))]
mod lifecycle_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[tokio::test]
    async fn reuses_authenticated_server_and_reaps_it_on_drop() {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("server.py");
        std::fs::write(
            &executable,
            r#"#!/usr/bin/env python3
import sys, os, json
from http.server import HTTPServer, BaseHTTPRequestHandler
if '--help' in sys.argv:
 print('--model'); sys.exit(0)
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  ok = self.headers.get('Authorization') == 'Bearer ' + os.environ['LLAMA_API_KEY']
  self.send_response(200 if ok else 401); self.end_headers()
  self.wfile.write(json.dumps({'pid':os.getpid()}).encode())
 def log_message(self, *args): pass
HTTPServer(('127.0.0.1', int(sys.argv[sys.argv.index('--port')+1])), Handler).serve_forever()
"#,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let model = root.path().join("model.gguf");
        std::fs::write(&model, "fixture").unwrap();
        let ini = root.path().join("models.ini");
        std::fs::write(&ini, format!("[fixture]\nmodel={}\n", model.display())).unwrap();
        let manifest: Manifest = serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{"name":"fixture"}, "connections":[],
            "nodes":[{"id":"llm", "node_type":"MultimodalLLMNode", "params":{
                "managed_server":{"executable":executable,"models_ini":ini,"preset":"fixture","gpu":"0","startup_timeout_secs":5}
            }}]
        })).unwrap();
        let owner = ManagedLlamaServers::default();
        let first = owner.prepare(&manifest).await.unwrap();
        let second = owner.prepare(&manifest).await.unwrap();
        let params = &first.nodes[0].params;
        assert_eq!(params, &second.nodes[0].params);
        assert!(params.get("max_tokens").is_none());
        let url = format!("{}/models", params["base_url"].as_str().unwrap());
        let client = reqwest::Client::new();
        assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
        let pid = client
            .get(&url)
            .bearer_auth(params["api_key"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()["pid"]
            .as_u64()
            .unwrap();
        drop(owner);
        let process = format!("/proc/{pid}");
        for _ in 0..50 {
            if !std::path::Path::new(&process).exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("managed server survived its executor owner");
    }
}
