use super::*;

fn hit() -> Evidence {
    serde_json::from_value(json!({"source":{"source_id":"S1","document_id":"harbor.md", "title":"Harbor",
        "document_revision":"a".repeat(64),"chunk_id":"b".repeat(64),"index_version":"c".repeat(64),"line_start":5,"line_end":7},
        "text":"The departure gate is K7.","score":0.8})).unwrap()
}

#[test]
fn config_separates_answer_backend_from_retrieval_policy() {
    let base =
        json!({"answer_backend":"server","rag_url":"http://127.0.0.1:8095","rag_token":"test"});
    let config = config_from_params(&base).unwrap();
    assert!(config.server);
    assert_eq!(config.policy, "direct");
    let mut phone = base.clone();
    phone["answer_backend"] = json!("phone");
    assert!(config_from_params(&phone).is_err());
    phone["model_path"] = json!("gemma.litertlm");
    phone["retrieval_policy"] = json!("agentic");
    assert!(!config_from_params(&phone).unwrap().server);
    for url in [
        "file:///secret",
        "http://user:pass@host",
        "http://host/private",
        "http://host?key=secret",
    ] {
        let mut value = base.clone();
        value["rag_url"] = json!(url);
        assert!(config_from_params(&value).is_err());
    }
    for key in ["rag_token", "answer_backend", "retrieval_policy"] {
        let mut value = base.clone();
        value[key] = json!("");
        assert!(config_from_params(&value).is_err());
    }
}

#[test]
fn evidence_and_citations_are_strict_and_speech_has_no_markers() {
    let value = json!([hit()]);
    let hits = protocol::evidence(&value, &"c".repeat(64)).unwrap();
    let line = protocol::sentence("The departure gate is K7 [S1].", &hits, "r1").unwrap();
    assert_eq!(line["text"], "The departure gate is K7.");
    assert_eq!(line["say"], line["text"]);
    assert_eq!(line["sources"][0]["document_id"], "harbor.md");
    for sentence in [
        "The gate is K7.",
        "The gate is K7 [S2].",
        "The gate [S1][S99].",
        "{\"tool\":\"x\"} [S1].",
    ] {
        assert!(protocol::sentence(sentence, &hits, "r1").is_err());
    }
    let mut bad = value.clone();
    bad[0]["source"]["document_id"] = json!("../secret");
    assert!(protocol::evidence(&bad, &"c".repeat(64)).is_err());
    assert!(protocol::evidence(&json!([hit(), hit()]), &"c".repeat(64)).is_err());
    assert!(protocol::evidence(&value, &"d".repeat(64)).is_err());
}

#[test]
fn phone_prompt_preserves_question_and_drops_history_before_evidence() {
    let question = "Which gate?";
    let history = vec![json!({"author":"Ana", "role":"person", "text":"x".repeat(7000)})];
    // A deliberately simple injected counter tests selection; real-model checks
    // separately exercise the runtime's generation tokenizer and chat template.
    let count = |text: &str| Ok(prompt::SYSTEM.len() + text.len());
    let (text, hits, tokens) =
        prompt::grounded(question, &history, &[hit()], 4096, 160, count).unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["current_question"], question);
    assert_eq!(value["history"], json!([]));
    assert_eq!(hits.len(), 1);
    assert!(tokens + 160 + 32 <= 4096);
    assert!(prompt::grounded(&"你".repeat(1600), &[], &[hit()], 4096, 160, count).is_err());
    let mut long = hit();
    long.text = "x".repeat(4000);
    assert!(prompt::grounded(question, &[], &[long], 4096, 160, count).is_err());
    assert!(matches!(
        prompt::grounded(question, &[], &[hit()], 4096, 160, |_| Err(
            "tokenizer failed".into()
        )),
        Err(prompt::BudgetError::Tokenizer(_))
    ));
    // Each construction includes only its current selected evidence.
    let mut next = hit();
    next.text = "The new gate is Z8.".into();
    let (text, _, _) = prompt::grounded(question, &[], &[next], 4096, 160, count).unwrap();
    assert!(!text.contains("K7"));
}

#[test]
#[ignore = "requires the pinned Gemma artifact and LiteRT-LM runtime"]
fn real_gemma_tokenizer_budgets_rendered_prompt_and_near_capacity() {
    let mut config = crate::lm::LmConfig::new(std::env::var("REMOTEMEDIA_GEMMA_LITERTLM").unwrap());
    config.runtime_lib = Some(std::env::var("REMOTEMEDIA_LITERT_LM_LIB").unwrap());
    config.backend = "cpu".into();
    let engine = crate::lm::Engine::load(&config).unwrap();
    let mut conversation = engine.conversation(prompt::SYSTEM).unwrap();
    let mut large = hit();
    large.text = "跨语言 evidence 🚢 ".repeat(4000);
    let history =
        vec![json!({"role":"person", "author":"Ana", "text":"old history ".repeat(4000)})];
    let (text, selected, tokens) = prompt::grounded(
        "Which gate?",
        &history,
        &[large.clone(), hit()],
        engine.max_tokens,
        160,
        |s| conversation.first_prompt_tokens(&engine, s),
    )
    .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].text, hit().text);
    assert!(tokens + 160 + 32 <= engine.max_tokens);
    assert_eq!(
        tokens,
        conversation.first_prompt_tokens(&engine, &text).unwrap()
    );
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["history"],
        json!([])
    );
    assert!(matches!(
        prompt::grounded("Which gate?", &[], &[large], engine.max_tokens, 160, |s| {
            conversation.first_prompt_tokens(&engine, s)
        }),
        Err(prompt::BudgetError::Context)
    ));
    // Validate the same prompt in real generation, including the template state
    // after repeated dry renders. Do not treat a token count as inference proof.
    let receiver = conversation.send(&text, 160).unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut answer = String::new();
    loop {
        match receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap()
        {
            Piece::Text(text) => answer.push_str(&text),
            Piece::Done => break,
            Piece::Error(error) => panic!("{error}"),
        }
    }
    eprintln!(
        "actual prompt tokens={tokens}, KV tokens={}, answer={answer:?}",
        conversation.tokens()
    );
    assert!(answer.contains("K7") && answer.contains("[S1]"));
    assert!(conversation.tokens() <= engine.max_tokens);
}

#[test]
fn streaming_holds_sentences_and_bounded_split_utf8_sse() {
    let mut sentences = Sentences::default();
    assert!(sentences.push("The gate is ").unwrap().is_empty());
    assert_eq!(
        sentences.push("K7 [S1]. Next ").unwrap(),
        vec!["The gate is K7 [S1]."]
    );
    assert_eq!(sentences.finish().as_deref(), Some("Next"));
    let value = json!({"text":"café"});
    let frame = format!("event: delta\r\ndata: {value}\r\n\r\n");
    let mut sse = protocol::Sse::default();
    let mut events = Vec::new();
    for byte in frame.as_bytes() {
        events.extend(sse.push(&[*byte]).unwrap());
    }
    assert_eq!(events, vec![value]);
    assert!(sse.complete());
    assert!(sse.push(&vec![b'x'; protocol::MAX_EVENT + 1]).is_err());
}

#[test]
fn history_only_policy_is_narrow_and_attributed() {
    let history = vec![
        json!({"author":"Ana","role":"person","text":"My code is amber."}),
        json!({"author":"Ben","role":"person","text":"My code is blue."}),
    ];
    assert_eq!(
        conversation_reply(
            "Pixel, what did I just say?",
            "Ana",
            &history,
            &["pixel".into()]
        )
        .as_deref(),
        Some("You said: My code is amber.")
    );
    assert!(conversation_reply(
        "Pixel, what is the harbor code?",
        "Ana",
        &history,
        &["pixel".into()]
    )
    .is_none());
}

#[test]
fn dropping_work_cancels_subwork_without_waiting() {
    let parent = CancelSource::new();
    let work = Work(parent.token().child());
    let token = work.0.token();
    drop(work);
    assert!(token.is_cancelled());
    assert!(!parent.is_cancelled());
}

mod http_fixture {
    use super::*;
    use remotemedia_plugin_sdk::traits::{CancelToken, MediaCapabilities};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    struct Fixture {
        url: String,
        mode: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Value>>>,
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Fixture {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            listener.set_nonblocking(true).unwrap();
            let mode = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (m, r, s) = (mode.clone(), requests.clone(), stop.clone());
            let thread = std::thread::spawn(move || {
                let mut workers = Vec::new();
                while !s.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let (m, r, s) = (m.clone(), r.clone(), s.clone());
                            workers.push(std::thread::spawn(move || {
                                let _ = serve(stream, m.load(Ordering::Relaxed), r, s);
                            }));
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(2)),
                    }
                }
                for worker in workers {
                    worker.join().unwrap();
                }
            });
            Self {
                url,
                mode,
                requests,
                stop,
                thread: Some(thread),
            }
        }
        fn node(&self) -> RagAssistantNode {
            RagAssistantNode::start(
                config_from_params(&json!({"answer_backend":"server", "rag_url":self.url,
                "rag_token":"fixture", "turn_timeout_ms":5000}))
                .unwrap(),
            )
            .unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.thread.take().unwrap().join().unwrap();
        }
    }
    fn serve(
        mut stream: TcpStream,
        mode: usize,
        requests: Arc<Mutex<Vec<Value>>>,
        stop: Arc<AtomicBool>,
    ) -> std::io::Result<()> {
        // Windows accepted sockets can inherit the listener's nonblocking mode.
        // This fixture intentionally uses blocking read_exact on worker threads.
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut bytes = Vec::new();
        let header_end;
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte)?;
            bytes.extend_from_slice(&byte);
            if bytes.ends_with(b"\r\n\r\n") {
                header_end = bytes.len();
                break;
            }
            if bytes.len() > 8192 {
                return Ok(());
            }
        }
        let headers = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length:")
                    .and_then(|s| s.trim().parse().ok())
            })
            .unwrap_or(0);
        if length > 128 * 1024 {
            return Ok(());
        }
        bytes.resize(header_end + length, 0);
        stream.read_exact(&mut bytes[header_end..])?;
        if headers.starts_with("get /health ") {
            let body = json!({"ready":true,"version":1,"collection_id":"room-docs", "capabilities":{"retrieve":true,"server_answer":true,"agentic":true}}).to_string();
            return write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
        }
        if headers.starts_with("delete ") {
            return write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            );
        }
        let request: Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
        requests.lock().unwrap().push(request.clone());
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
        )?;
        let frame = |kind: &str, mut fields: Value| {
            for key in [
                "version",
                "request_id",
                "instance_id",
                "session_id",
                "member_id",
                "reply_id",
                "line_seq",
            ] {
                fields[key] = request[key].clone();
            }
            fields["kind"] = json!(kind);
            fields["index_version"] = json!("c".repeat(64));
            format!("event: {kind}\ndata: {fields}\n\n")
        };
        write!(stream, "{}", frame("evidence", json!({"evidence":[hit()]})))?;
        let text = if mode == 2 {
            "The gate is invented [S8]. "
        } else {
            "The gate is K7 [S1]. "
        };
        write!(stream, "{}", frame("delta", json!({"text":text})))?;
        stream.flush()?;
        if mode == 1 {
            for _ in 0..200 {
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        write!(
            stream,
            "{}",
            frame("outcome", json!({"outcome":"answered","evidence":[hit()]}))
        )
    }

    struct Ctx(CancelToken);
    impl NodeRuntimeContextRead for Ctx {
        fn session_id(&self) -> &str {
            "test-room"
        }
        fn node_id(&self) -> &str {
            "assistant"
        }
        fn cancel_token(&self) -> CancelToken {
            self.0.clone()
        }
        fn capability(&self, _: &str) -> Option<MediaCapabilities> {
            None
        }
        fn insert_capability(&self, _: &str, _: MediaCapabilities) {}
        fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            None
        }
        fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            None
        }
        fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            None
        }
    }
    fn question() -> RuntimeData {
        RuntimeData::Json(json!({"kind":"text","text":"Pixel, which gate should I use?","seq":7}))
    }
    async fn run(node: &RagAssistantNode, data: RuntimeData) -> Vec<Value> {
        let mut output = Vec::new();
        node.process_streaming_with_ctx(data, &Ctx(CancelToken::never()), |value| {
            if let RuntimeData::Json(value) = value {
                output.push(value);
            }
            Ok(())
        })
        .await
        .unwrap();
        output
    }

    #[tokio::test]
    async fn disposable_source_probe_publishes_exact_metadata_without_generation() {
        let fixture = Fixture::start();
        let nonce = "a".repeat(32);
        let params = json!({"answer_backend":"server", "rag_url":fixture.url, "rag_token":"fixture",
            "respond":"all", "_source_probe":{"nonce":nonce,"evidence":[hit()]}});
        let node = RagAssistantNode::start(config_from_params(&params).unwrap()).unwrap();
        let output = run(
            &node,
            RuntimeData::Json(json!({"kind":"text","text":nonce,"seq":1})),
        )
        .await;
        let line = output.iter().find(|v| v["kind"] == "text").unwrap();
        assert_eq!(line["text"], format!("Source metadata check {nonce}."));
        assert_eq!(line["sources"], json!([hit().source]));
        assert!(fixture.requests.lock().unwrap().is_empty());
        let mut invalid = params;
        invalid["_source_probe"]["nonce"] = json!("unbounded or guessable");
        assert!(config_from_params(&invalid).is_err());
    }

    #[tokio::test]
    async fn server_mode_runs_without_gemma_and_gates_before_http() {
        let fixture = Fixture::start();
        let node = fixture.node();
        let history = RuntimeData::Json(
            json!({"kind":"text","text":"Pixel, old question?","seq":1,"history":true}),
        );
        assert!(run(&node, history).await.is_empty());
        assert_eq!(
            run(&node, RuntimeData::Text("Hey Pixel".into())).await[0]["state"],
            "listening"
        );
        assert!(fixture.requests.lock().unwrap().is_empty());
        let output = run(&node, question()).await;
        assert_eq!(output[0]["state"], "thinking");
        assert_eq!(output[1]["say"], "The gate is K7.");
        assert_eq!(output[1]["sources"][0]["source_id"], "S1");
        assert_eq!(output.last().unwrap()["state"], "idle");
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["line_seq"], 7);
        assert_eq!(requests[0]["history"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn bad_citations_never_reach_speech() {
        let fixture = Fixture::start();
        fixture.mode.store(2, Ordering::Relaxed);
        let node = fixture.node();
        let output = run(&node, question()).await;
        assert!(output
            .iter()
            .all(|line| !line["say"].as_str().unwrap_or("").contains("invented")));
        assert_eq!(output[1]["say"], failure_message("invalid_output").unwrap());
    }

    #[tokio::test]
    async fn replacement_reconstructs_history_and_retrieves_with_fresh_identity() {
        let fixture = Fixture::start();
        let original = fixture.node();
        run(&original, question()).await;
        let first = fixture.requests.lock().unwrap()[0].clone();
        drop(original);

        let replacement = fixture.node();
        let mut old_source = serde_json::to_value(hit().source).unwrap();
        old_source["index_version"] = json!("e".repeat(64));
        for line in [
            json!({"kind":"text","text":"Pixel, which gate should I use?","seq":7,"history":true}),
            json!({"kind":"text","text":"The gate is K7.","seq":8,"history":true,
                   "author_kind":"agent","interrupted":true,"sources":[old_source]}),
            json!({"kind":"text","text":"Hey Pixel","seq":9,"history":true}),
        ] {
            assert!(run(&replacement, RuntimeData::Json(line)).await.is_empty());
        }
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
        // The paired-session host replays unsettled input as a live line. A bare
        // question must be accepted after the restored name-only mention.
        let output = run(
            &replacement,
            RuntimeData::Json(json!({
                "kind":"text","text":"Which gate should I use?","seq":10,
            })),
        )
        .await;
        assert_eq!(output[1]["say"], "The gate is K7.");
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let replay = &requests[1];
        assert_ne!(first["request_id"], replay["request_id"]);
        assert_ne!(first["instance_id"], replay["instance_id"]);
        assert_ne!(first["reply_id"], replay["reply_id"]);
        assert_eq!(first["session_id"], replay["session_id"]);
        assert_eq!(replay["line_seq"], 10);
        assert!(replay["question"]
            .as_str()
            .unwrap()
            .ends_with("Which gate should I use?"));
        let history = replay["history"].as_array().unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[1]["role"], "agent");
        assert_eq!(history[1]["interrupted"], true);
        // Old citations remain room-log provenance, never fresh evidence in a
        // request. The replacement obtains the current snapshot from the service.
        assert!(history.iter().all(|line| line.get("sources").is_none()));
        assert_eq!(
            output[1]["sources"][0]["index_version"],
            hit().source.index_version
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_during_remote_generation_releases_next_turn() {
        let fixture = Fixture::start();
        fixture.mode.store(1, Ordering::Relaxed);
        let node = Arc::new(fixture.node());
        let source = CancelSource::new();
        let ctx = Ctx(source.token());
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let n = node.clone();
        let task = tokio::spawn(async move {
            n.process_streaming_with_ctx(question(), &ctx, |value| {
                let _ = sent.send(value);
                Ok(())
            })
            .await
        });
        loop {
            if matches!(received.recv().await, Some(RuntimeData::Json(v)) if v.get("say").is_some())
            {
                break;
            }
        }
        source.cancel(Cancellation::new(CancelReason::BargeIn));
        task.abort();
        let _ = task.await;
        fixture.mode.store(0, Ordering::Relaxed);
        let output = tokio::time::timeout(Duration::from_millis(500), run(&node, question()))
            .await
            .expect("old HTTP request blocked the next turn");
        assert_eq!(output[1]["say"], "The gate is K7.");
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_ne!(requests[0]["request_id"], requests[1]["request_id"]);
        assert!(requests[1]["history"]
            .as_array()
            .unwrap()
            .iter()
            .any(|line| line["interrupted"] == true));
    }
}
