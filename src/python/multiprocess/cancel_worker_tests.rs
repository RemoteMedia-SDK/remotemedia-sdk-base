//! A generator's call cancelled through a real Python worker process
//! (openspec: add-generator-cancellation).
//!
//! Real executor, real iceoryx2 channels, real Python worker running
//! `SlowTextGenerator` (`tests/support/slow_generator_node.py`): the worker
//! announces its role in the `META:` handshake, and a `__cancel_call__` frame
//! sent out of band while a reply streams stops it, runs the node's
//! `cancel()` with the re-resolved disposition, and ends the turn. Runs only
//! with `REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1` and
//! `REMOTEMEDIA_TEST_PYTHON_CLIENT=<remotemedia-sdk>/clients/python`
//! (`PYTHON_EXECUTABLE` selects the interpreter), like the IPC release tests.

use super::*;
use crate::nodes::python_streaming::cancel_call_frame;
use remotemedia_traits::{
    CancelReason, CancelRequest, CancelStrategy, Cancellation, Disposition, GeneratedModality,
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const NODE: &str = "gen";
/// The fixture's words are 50 ms apart; a reply runs 5 s.
const FULL_REPLY: Duration = Duration::from_secs(5);

fn enabled() -> Option<PathBuf> {
    if std::env::var("REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping: set REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1 and REMOTEMEDIA_TEST_PYTHON_CLIENT");
        return None;
    }
    Some(PathBuf::from(
        std::env::var("REMOTEMEDIA_TEST_PYTHON_CLIENT").expect("REMOTEMEDIA_TEST_PYTHON_CLIENT"),
    ))
}

type Words = Arc<std::sync::Mutex<Vec<(Instant, String)>>>;

/// One turn, run in the background; its words land in the returned list.
fn turn(
    executor: &Arc<MultiprocessExecutor>,
    session: &str,
    text: &str,
) -> (Words, tokio::task::JoinHandle<Result<usize>>) {
    let words: Words = Default::default();
    let sink = words.clone();
    let (executor, session, text) = (executor.clone(), session.to_string(), text.to_string());
    let task = tokio::spawn(async move {
        executor
            .process_runtime_data_streaming(
                crate::data::RuntimeData::Text(text),
                Some(session),
                move |output| {
                    if let crate::data::RuntimeData::Text(word) = output {
                        sink.lock().unwrap().push((Instant::now(), word));
                    }
                    Ok(())
                },
            )
            .await
    });
    (words, task)
}

async fn until(words: &Words, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while words.lock().unwrap().len() < count {
        assert!(Instant::now() < deadline, "the reply did not start");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

struct Cancelled {
    total: usize,
    after_cancel: usize,
    ended_after: Duration,
}

/// Let a turn stream a few words, cancel it out of band, and wait for the
/// turn to end.
async fn cancel_mid_reply(
    executor: &Arc<MultiprocessExecutor>,
    session: &str,
    text: &str,
    request: CancelRequest,
) -> (Cancelled, String) {
    let (words, task) = turn(executor, session, text);
    until(&words, 5).await;
    let first = words.lock().unwrap()[0].1.clone();
    let frame = crate::data::RuntimeData::Text(cancel_call_frame(&request));
    let cancelled_at = Instant::now();
    MultiprocessExecutor::try_send_out_of_band(session, NODE, frame).expect("cancel frame");
    let ended = tokio::time::timeout(FULL_REPLY * 2, task)
        .await
        .expect("the cancelled turn did not end")
        .expect("turn task")
        .expect("turn");
    let ended_after = cancelled_at.elapsed();
    let words = words.lock().unwrap();
    let after_cancel = words.iter().filter(|(at, _)| *at > cancelled_at).count();
    assert_eq!(ended, words.len());
    (
        Cancelled {
            total: words.len(),
            after_cancel,
            ended_after,
        },
        first,
    )
}

fn log_lines(log: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_frame_stops_a_real_python_generator() {
    let Some(client) = enabled() else { return };
    let session = format!("cancel{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let log = std::env::temp_dir().join(format!("{session}-cancels.jsonl"));
    let mut config = MultiprocessConfig::default();
    config.init_timeout_secs = 60;
    if let Ok(python) = std::env::var("PYTHON_EXECUTABLE") {
        config.python_executable = python.into();
    }
    config.python_path = vec![client.clone(), client.join("tests").join("support")];
    let mut executor = MultiprocessExecutor::new(config);
    executor.register_module("slow_generator_node").await;
    let ctx = ExecutorNodeContext {
        node_id: NODE.into(),
        node_type: "SlowTextGenerator".into(),
        params: serde_json::json!({"cancel_log": log}),
        session_id: Some(session.clone()),
        metadata: HashMap::new(),
    };
    ExecutorNodeExecutor::initialize(&mut executor, &ctx)
        .await
        .expect("session");

    // The worker announced its role before READY.
    let (declared, dispositions) = executor.declared_generator();
    assert_eq!(declared.generates, Some(GeneratedModality::Text));
    assert_eq!(declared.cancel, Some(CancelStrategy::ObservesToken));
    assert!(
        dispositions.is_some(),
        "the META handshake carries default dispositions"
    );

    let executor = Arc::new(executor);

    // Barge-in as the router forwards it: the node re-resolves with its own
    // default (interrupt): the cut-off reply is kept.
    let (barge, first) = cancel_mid_reply(
        &executor,
        &session,
        "a",
        CancelRequest::new(Cancellation::new(CancelReason::BargeIn)),
    )
    .await;
    assert!(first.starts_with("h0:a:"), "{first}");
    eprintln!(
        "barge_in: {} words, {} after the cancel, turn ended {:?} after it",
        barge.total, barge.after_cancel, barge.ended_after
    );
    assert!(barge.total < 100 && barge.after_cancel <= 2);
    assert!(
        barge.ended_after < Duration::from_secs(1),
        "{:?}",
        barge.ended_after
    );

    // The next call answers at once, with the interrupted reply in history.
    let (abort, first) = cancel_mid_reply(
        &executor,
        &session,
        "b",
        CancelRequest::exact(Cancellation::with_disposition(
            CancelReason::CallerCancelled,
            Disposition::ABORT,
        )),
    )
    .await;
    assert!(
        first.starts_with("h1:b:"),
        "interrupt kept the cut-off reply: {first}"
    );
    eprintln!(
        "abort: {} words, {} after the cancel, turn ended {:?} after it",
        abort.total, abort.after_cancel, abort.ended_after
    );
    assert!(abort.total < 100 && abort.after_cancel <= 2);
    assert!(
        abort.ended_after < Duration::from_secs(1),
        "{:?}",
        abort.ended_after
    );

    // Abort discarded the second cut-off reply.
    let (words, task) = turn(&executor, &session, "c");
    until(&words, 1).await;
    let first = words.lock().unwrap()[0].1.clone();
    assert!(
        first.starts_with("h1:c:"),
        "abort discarded the cut-off reply: {first}"
    );
    let frame = crate::data::RuntimeData::Text(cancel_call_frame(&CancelRequest::exact(
        Cancellation::new(CancelReason::Shutdown),
    )));
    MultiprocessExecutor::try_send_out_of_band(&session, NODE, frame).expect("cancel frame");
    tokio::time::timeout(FULL_REPLY * 2, task)
        .await
        .expect("the third turn did not end")
        .unwrap()
        .unwrap();

    // cancel() ran in the worker once per call, with the resolved reason
    // and disposition.
    let lines = log_lines(&log);
    eprintln!("cancel log: {lines:#?}");
    assert_eq!(lines.len(), 3, "{lines:?}");
    assert_eq!(lines[0]["cancellation"]["reason"], "barge_in");
    assert_eq!(
        lines[0]["cancellation"]["disposition"]["partial_output"],
        "retain"
    );
    assert_eq!(lines[0]["kept"], true);
    assert_eq!(lines[1]["cancellation"]["reason"], "caller_cancelled");
    assert_eq!(
        lines[1]["cancellation"]["disposition"]["partial_output"],
        "discard"
    );
    assert_eq!(lines[1]["kept"], false);
    assert_eq!(lines[2]["cancellation"]["reason"], "shutdown");

    let mut executor = Arc::into_inner(executor).expect("no turn holds the executor");
    ExecutorNodeExecutor::cleanup(&mut executor).await.unwrap();
    let _ = std::fs::remove_file(&log);
}
