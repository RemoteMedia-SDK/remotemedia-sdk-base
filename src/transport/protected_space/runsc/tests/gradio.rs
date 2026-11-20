use super::*;

const APP: &str = r#"
from pathlib import Path
import sys
import time
import gradio as gr

def respond(file):
    text = Path(file).read_text()
    marker = Path(sys.prefix) / ".fixture-venv-marker"
    previous = marker.read_text() if marker.exists() else "fresh"
    marker.write_text(text)
    yield f"started {text} (venv: {previous})"
    time.sleep(0.1)
    yield f"finished {text}"

with gr.Blocks(analytics_enabled=False) as app:
    source = gr.File(type="filepath")
    result = gr.Textbox()
    gr.Button("Run").click(respond, source, result, api_name="respond")

app.queue().launch()
"#;

#[tokio::test]
#[ignore = "requires a trusted Gradio guest image, locked wheelhouse and Linux sandbox fixture"]
async fn gradio_upload_and_stream_from_locked_wheels() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let registry = Arc::new(SpaceArtifactRegistry::open(root.path().join("artifacts")).unwrap());
    let rootfs = fixture_path("RM_SPACE_TEST_GRADIO_ROOTFS");
    let wheel_dir = fixture_path("RM_SPACE_TEST_GRADIO_WHEELS");
    let executable = fixture_path("RM_SPACE_TEST_RUNSC");
    let delegated_cgroup = fixture_path("RM_SPACE_TEST_CGROUP");
    let executor = PipelineExecutor::for_protected_spaces().unwrap();
    let mut wheels = std::collections::BTreeMap::new();
    for entry in fs::read_dir(wheel_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|suffix| suffix == "whl") {
            let bytes = fs::read(&path).unwrap();
            let digest = hash(&bytes);
            registry
                .import_blob(&digest, bytes.len() as u64, &bytes[..])
                .unwrap();
            wheels.insert(
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                json!({"size":bytes.len(), "sha256":digest}),
            );
        }
    }
    assert!(wheels.contains_key("gradio-6.14.0-py3-none-any.whl"));
    let source_digest = hash(APP.as_bytes());
    registry
        .import_blob(&source_digest, APP.len() as u64, APP.as_bytes())
        .unwrap();
    let tree = json!([["app.py", "file", APP.len(), 0, source_digest]]).to_string();
    let python = std::env::var("RM_SPACE_TEST_GRADIO_PYTHON").unwrap_or_else(|_| "3.12".into());
    let artifact = json!({
        "version":1, "repository":"org/demo", "commit":"a".repeat(40), "sdk":"gradio", "entry_file":"app.py",
        "source_tree_sha256":hash(tree.as_bytes()),
        "target":{"platform":"linux", "arch":"x86_64", "python":python, "abi":"cp312", "backend":"cpu"},
        "wheels":wheels, "models":[],
    });
    let execution = authorize_artifact(&executor, &registry, &artifact, &tree, "original");
    let sibling = authorize_artifact(&executor, &registry, &artifact, &tree, "sibling");
    let mut wrong_version = artifact;
    wrong_version["target"]["python"] = json!("3.12.999");
    let wrong_version =
        authorize_artifact(&executor, &registry, &wrong_version, &tree, "wrong-python");

    let runtime = IsolatedSpaceRuntime::open(
        registry,
        IsolatedSpaceConfig {
            runtime: RunscConfig {
                executable_sha256: hash(&fs::read(&executable).unwrap()),
                executable,
                rootfs,
                state: state.clone(),
                delegated_cgroup,
                relay_program: "/opt/remotemedia/relay".into(),
            },
            launcher: "/opt/remotemedia/remotemedia-space-launch".into(),
            python_abi: "cp312".into(),
            loopback_port: 24623,
            limits: WorkloadLimits {
                cpu_millis: 2000,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                scratch_bytes: 768 * 1024 * 1024,
                processes: 256,
                lifetime: Duration::from_secs(120),
            },
        },
    )
    .await
    .unwrap();
    let rejected = runtime.stage(&wrong_version).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        match rejected.start(&wrong_version).await {
            Ok(run) => assert!(
                matches!(run.wait().await.unwrap(), WorkloadExit::Exited(code) if code != 0)
            ),
            Err(error @ IsolatedSpaceError::Sandbox(_)) => {
                assert!(!error.cleanup_pending(), "{error}")
            }
            Err(error) => panic!("unexpected interpreter mismatch outcome: {error}"),
        }
    })
    .await
    .expect("an image with a mismatched Python patch stayed active");
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);
    let original = runtime.stage(&execution).await.unwrap();
    let staged_sibling = runtime.stage(&sibling).await.unwrap();
    let (run, sibling_run) =
        tokio::try_join!(original.start(&execution), staged_sibling.start(&sibling)).unwrap();
    tokio::join!(
        wait_for_gradio(&run, &execution),
        wait_for_gradio(&sibling_run, &sibling)
    );
    assert!(matches!(
        run.open_stream(&sibling),
        Err(IsolatedSpaceError::WrongRun)
    ));
    assert_inference(&run, &execution, "original upload", "fresh").await;
    assert_inference(&sibling_run, &sibling, "sibling upload", "fresh").await;
    assert_eq!(run.stop().await.unwrap(), WorkloadExit::Stopped);
    assert_inference(&sibling_run, &sibling, "later upload", "sibling upload").await;
    assert_eq!(sibling_run.stop().await.unwrap(), WorkloadExit::Stopped);
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);
}

async fn wait_for_gradio(run: &IsolatedSpaceRun, execution: &VerifiedSpaceExecution) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(reply) = request(run, execution, "GET", "/config", "", "").await {
                if reply.starts_with("HTTP/1.1 200 ") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("isolated Gradio did not start");
}

async fn assert_inference(
    run: &IsolatedSpaceRun,
    execution: &VerifiedSpaceExecution,
    text: &str,
    previous: &str,
) {
    let upload = format!("--rm-test\r\nContent-Disposition: form-data; name=\"files\"; filename=\"example.txt\"\r\nContent-Type: text/plain\r\n\r\n{text}\r\n--rm-test--\r\n");
    let reply = request(
        run,
        execution,
        "POST",
        "/gradio_api/upload",
        "multipart/form-data; boundary=rm-test",
        &upload,
    )
    .await
    .unwrap();
    let paths: Vec<String> = serde_json::from_str(body(&reply)).unwrap();
    assert_eq!(paths.len(), 1);
    let input = json!({"data":[{"path":paths[0], "meta":{"_type":"gradio.FileData"}}]}).to_string();
    let reply = request(
        run,
        execution,
        "POST",
        "/gradio_api/call/respond",
        "application/json",
        &input,
    )
    .await
    .unwrap();
    let call: serde_json::Value = serde_json::from_str(body(&reply)).unwrap();
    let result_path = format!(
        "/gradio_api/call/respond/{}",
        call["event_id"].as_str().unwrap()
    );
    let events = request(run, execution, "GET", &result_path, "", "")
        .await
        .unwrap();
    assert!(events.contains("text/event-stream"), "{events}");
    let first = events
        .find(&format!("started {text} (venv: {previous})"))
        .expect(&events);
    let last = events.find(&format!("finished {text}")).expect(&events);
    assert!(
        first < last && events.contains("event: complete"),
        "{events}"
    );
}

fn authorize_artifact(
    executor: &PipelineExecutor,
    registry: &SpaceArtifactRegistry,
    artifact: &serde_json::Value,
    tree: &str,
    run_id: &str,
) -> VerifiedSpaceExecution {
    let artifact = artifact.to_string();
    let digest = hash(artifact.as_bytes());
    let (mut approval, controller, manifest) = fixture(ProtectedSpacePhase::Serve {
        artifact_sha256: digest.clone(),
    });
    approval.run_id = run_id.into();
    approval.session_id = format!("session-{run_id}");
    let scope = SpaceArtifactScope {
        artifact_sha256: digest,
        repository: approval.repository.clone(),
        commit: approval.commit.clone(),
        target: approval.target.clone(),
        profile: "isolated".into(),
        policy_revision: approval.policy_revision.clone(),
    };
    registry.register(&scope, &artifact, tree).unwrap();
    let bytes = encode(&mut approval, &manifest);
    executor
        .verify_protected_space(approval, &controller, &bytes)
        .unwrap()
}

fn fixture_path(variable: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(variable).unwrap_or_else(|| panic!("set {variable}")))
}

fn body(reply: &str) -> &str {
    assert!(reply.starts_with("HTTP/1.1 200 "), "{reply}");
    reply.split_once("\r\n\r\n").unwrap().1
}

async fn request(
    run: &IsolatedSpaceRun,
    execution: &VerifiedSpaceExecution,
    method: &str,
    path: &str,
    content_type: &str,
    body: &str,
) -> anyhow::Result<String> {
    let mut stream = run.open_stream(execution)?;
    let message = format!("{method} {path} HTTP/1.0\r\nHost: 127.0.0.1:24623\r\nConnection: close\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{body}", body.len());
    tokio::time::timeout(Duration::from_secs(15), async {
        stream.input.write_all(message.as_bytes()).await?;
        let mut reply = String::new();
        stream.output.read_to_string(&mut reply).await?;
        Ok::<_, std::io::Error>(reply)
    })
    .await?
    .map_err(Into::into)
}
