//! `create_shared_streaming_session` shares one pipeline session per key in a
//! process, and `recv_event` attributes outputs (openspec:
//! add-multi-participant-sessions, multi-participant-ingress).
use super::*;
use pyo3::types::{PyDict, PyString};
use remotemedia_core::transport::ExecutorConfig;

fn manifest() -> String {
    serde_json::json!({"version": "v1", "metadata": {}, "nodes": [
        {"id": "echo", "node_type": "PassThrough", "params": {}}
    ], "connections": []})
    .to_string()
}

async fn open(key: &str, who: &str) -> PyResult<PyStreamingSession> {
    let future = Python::attach(|py| {
        pyo3_async_runtimes::tokio::into_future(create_shared_streaming_session(
            py,
            manifest(),
            key.to_string(),
            who.to_string(),
            "user".to_string(),
            None,
            None,
            None,
            None,
        )?)
    })?;
    let session = future.await?;
    Python::attach(|py| Ok(session.extract::<PyStreamingSession>(py)?))
}

async fn say(session: &PyStreamingSession, text: &str) -> PyResult<()> {
    let future = Python::attach(|py| {
        pyo3_async_runtimes::tokio::into_future(session.send_input(
            py,
            PyString::new(py, text).into_any(),
            false,
        )?)
    })?;
    future.await?;
    Ok(())
}

/// `(participant id, text)` of the next `recv_event`.
async fn event(session: &PyStreamingSession) -> PyResult<(String, String)> {
    let future = Python::attach(|py| pyo3_async_runtimes::tokio::into_future(session.recv_event(py)?))?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("event in time")?;
    Python::attach(|py| {
        let event = event.bind(py).cast::<PyDict>()?.clone();
        assert_eq!(event.get_item("kind")?.unwrap().extract::<String>()?, "data");
        let text = event.get_item("data")?.unwrap().extract::<String>()?;
        let who = event.get_item("participant")?.unwrap();
        let who = who.cast::<PyDict>()?;
        assert_eq!(who.get_item("role")?.unwrap().extract::<String>()?, "user");
        Ok((who.get_item("id")?.unwrap().extract::<String>()?, text))
    })
}

async fn close(session: &PyStreamingSession) -> PyResult<()> {
    let future = Python::attach(|py| pyo3_async_runtimes::tokio::into_future(session.close(py)?))?;
    future.await?;
    Ok(())
}

#[test]
fn two_handles_on_one_key_share_one_session_and_outputs_name_the_speaker() {
    Python::attach(|py| {
        pyo3_async_runtimes::tokio::run(py, async {
            let key = format!("ffi-room-{}", std::process::id());
            let alice = open(&key, "alice").await?;
            let bob = open(&key, "bob").await?;
            assert_eq!(alice.session_id, bob.session_id, "one session for the key");
            let elsewhere = open(&format!("{key}-other"), "carol").await?;
            assert_ne!(elsewhere.session_id, alice.session_id);
            close(&elsewhere).await?;

            say(&alice, "hi from alice").await?;
            assert_eq!(event(&alice).await?, ("alice".into(), "hi from alice".into()));
            assert_eq!(event(&bob).await?, ("alice".into(), "hi from alice".into()));
            say(&bob, "hello from bob").await?;
            assert_eq!(event(&alice).await?, ("bob".into(), "hello from bob".into()));
            assert_eq!(event(&bob).await?, ("bob".into(), "hello from bob".into()));

            // Alice leaves; Bob keeps the session; the last handle closes it.
            let executor = crate::shared_sessions::shared_executor(ExecutorConfig::default()).unwrap();
            let shared = executor.shared_session(&key).await.expect("shared session");
            close(&alice).await?;
            assert!(shared.is_active().await);
            say(&bob, "still here").await?;
            assert_eq!(event(&bob).await?, ("bob".into(), "still here".into()));
            close(&bob).await?;
            assert!(!shared.is_active().await, "last handle closed the session");
            Ok(())
        })
        .unwrap()
    });
}
