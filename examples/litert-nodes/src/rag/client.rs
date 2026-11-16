//! Own a Tokio reactor on a dedicated thread: the plugin does not assume a host reactor.
use super::{
    protocol::{correlated, Sse, MAX_EVENT},
    Config,
};
use futures_util::StreamExt;
use remotemedia_plugin_sdk::traits::CancelToken;
use serde_json::Value;
use std::{sync::mpsc, time::Duration};

pub enum Event {
    Wire(Value),
    Failure(&'static str),
}
pub struct Job {
    pub request: Value,
    pub cancel: CancelToken,
    pub output: tokio::sync::mpsc::Sender<Event>,
}
pub struct Client {
    jobs: tokio::sync::mpsc::Sender<Job>,
}
impl Client {
    pub fn start(config: &Config) -> Result<Self, String> {
        let (jobs, mut receiver) = tokio::sync::mpsc::channel::<Job>(1);
        let (ready, loaded) = mpsc::sync_channel(1);
        let config = config.clone();
        std::thread::Builder::new()
            .name("rag-http".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = ready.send(Err("cannot start RAG HTTP runtime".to_owned()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let client = match reqwest::Client::builder()
                        .redirect(reqwest::redirect::Policy::none())
                        .connect_timeout(Duration::from_secs(3))
                        .no_proxy()
                        .build()
                    {
                        Ok(client) => client,
                        Err(_) => {
                            let _ = ready.send(Err("cannot create RAG HTTP client".to_owned()));
                            return;
                        }
                    };
                    if let Err(error) = health(&client, &config).await {
                        let _ = ready.send(Err(error));
                        return;
                    }
                    let _ = ready.send(Ok(()));
                    let mut cleanup = tokio::task::JoinSet::new();
                    while let Some(job) = receiver.recv().await {
                        if job.cancel.is_cancelled() || job.output.is_closed() {
                            continue;
                        }
                        let result = tokio::select! {
                            biased;
                            _ = job.cancel.cancelled() => Err("cancelled"),
                            _ = job.output.closed() => Err("cancelled"),
                            result = exchange(&client, &config, &job) => result,
                        };
                        if let Err(outcome) = result {
                            if outcome == "cancelled" {
                                while cleanup.try_join_next().is_some() {}
                                if cleanup.len() < 8 {
                                    let client = client.clone();
                                    let config = config.clone();
                                    let request = job.request.clone();
                                    cleanup.spawn(async move {
                                        cancel(&client, &config, &request).await;
                                    });
                                }
                            } else {
                                let _ = job.output.send(Event::Failure(outcome)).await;
                            }
                        }
                    }
                    cleanup.abort_all();
                });
            })
            .map_err(|_| "cannot start RAG HTTP thread")?;
        loaded.recv().map_err(|_| "RAG HTTP startup exited")??;
        Ok(Self { jobs })
    }
    pub fn send(&self, job: Job) -> Result<(), String> {
        self.jobs
            .try_send(job)
            .map_err(|_| "RAG adapter is busy".into())
    }
}

async fn read_json(response: reqwest::Response) -> Result<Value, &'static str> {
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            if e.is_timeout() {
                "timeout"
            } else {
                "unavailable"
            }
        })?;
        if bytes.len() + chunk.len() > MAX_EVENT {
            return Err("invalid_output");
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid_output")
}

async fn health(client: &reqwest::Client, config: &Config) -> Result<(), String> {
    let response = client
        .get(format!("{}/health", config.url))
        .bearer_auth(&config.token)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|_| "RAG service is unreachable")?;
    if !response.status().is_success() {
        return Err("RAG service readiness/authentication failed".into());
    }
    let value = read_json(response).await?;
    if value["ready"] != true
        || value["version"] != 1
        || value["collection_id"] != config.collection
        || value["capabilities"]["retrieve"] != true
        || (config.server
            && config.source_probe.is_none()
            && value["capabilities"]["server_answer"] != true)
        || (config.policy == "agentic" && value["capabilities"]["agentic"] != true)
    {
        return Err("RAG service does not support the configured mode or collection".into());
    }
    Ok(())
}

async fn exchange(
    client: &reqwest::Client,
    config: &Config,
    job: &Job,
) -> Result<(), &'static str> {
    let timeout = Duration::from_millis(
        job.request["deadline_ms"]
            .as_u64()
            .ok_or("invalid_output")?,
    );
    let path = if config.server { "answer" } else { "retrieve" };
    let response = client
        .post(format!("{}/v1/{path}", config.url))
        .bearer_auth(&config.token)
        .timeout(timeout)
        .json(&job.request)
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                "timeout"
            } else {
                "unavailable"
            }
        })?;
    if !response.status().is_success() {
        return Err("unavailable");
    }
    if !config.server {
        let mut value = read_json(response).await?;
        if !correlated(&value, &job.request) {
            return Err("invalid_output");
        }
        value["kind"] = Value::String("outcome".into());
        job.output
            .send(Event::Wire(value))
            .await
            .map_err(|_| "cancelled")?;
        return Ok(());
    }
    if !response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/event-stream"))
    {
        return Err("invalid_output");
    }
    let mut stream = response.bytes_stream();
    let mut parser = Sse::default();
    let mut terminal = false;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes.map_err(|e| {
            if e.is_timeout() {
                "timeout"
            } else {
                "unavailable"
            }
        })?;
        for value in parser.push(&bytes).map_err(|_| "invalid_output")? {
            if terminal || !correlated(&value, &job.request) {
                return Err("invalid_output");
            }
            terminal = value["kind"] == "outcome";
            job.output
                .send(Event::Wire(value))
                .await
                .map_err(|_| "cancelled")?;
        }
    }
    if !terminal || !parser.complete() {
        return Err("invalid_output");
    }
    Ok(())
}

async fn cancel(client: &reqwest::Client, config: &Config, request: &Value) {
    let mut delete = client
        .delete(format!(
            "{}/v1/requests/{}",
            config.url,
            request["request_id"].as_str().unwrap_or("")
        ))
        .bearer_auth(&config.token)
        .timeout(Duration::from_secs(1));
    for (header, key) in [
        ("x-session-id", "session_id"),
        ("x-member-id", "member_id"),
        ("x-instance-id", "instance_id"),
    ] {
        delete = delete.header(header, request[key].as_str().unwrap_or(""));
    }
    let _ = delete.send().await;
}
