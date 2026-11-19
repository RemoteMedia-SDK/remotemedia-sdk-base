use super::*;

const APP: &str = r#"
from pathlib import Path
import time
import gradio as gr

def respond(file):
    text = Path(file).read_text()
    yield f"started {text}"
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
    let artifact = json!({
        "version":1, "repository":"org/demo", "commit":"a".repeat(40), "sdk":"gradio", "entry_file":"app.py",
        "source_tree_sha256":hash(tree.as_bytes()),
        "target":{"platform":"linux", "arch":"x86_64", "python":"3.12", "abi":"cp312", "backend":"cpu"},
        "wheels":wheels, "models":[],
    }).to_string();
    let digest = hash(artifact.as_bytes());
    let (mut approval, controller, manifest) = fixture(ProtectedSpacePhase::Serve {
        artifact_sha256: digest.clone(),
    });
    let scope = SpaceArtifactScope {
        artifact_sha256: digest,
        repository: approval.repository.clone(),
        commit: approval.commit.clone(),
        target: approval.target.clone(),
        profile: "isolated".into(),
        policy_revision: approval.policy_revision.clone(),
    };
    registry.register(&scope, &artifact, &tree).unwrap();
    let bytes = encode(&mut approval, &manifest);
    let execution = executor
        .verify_protected_space(approval, &controller, &bytes)
        .unwrap();

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
    let run = runtime
        .stage(&execution)
        .await
        .unwrap()
        .start(&execution)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(reply) = request(&run, &execution, "GET", "/config", "", "").await {
                if reply.starts_with("HTTP/1.1 200 ") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("isolated Gradio did not start");

    let upload = "--rm-test\r\nContent-Disposition: form-data; name=\"files\"; filename=\"example.txt\"\r\nContent-Type: text/plain\r\n\r\nverified upload\r\n--rm-test--\r\n";
    let reply = request(
        &run,
        &execution,
        "POST",
        "/gradio_api/upload",
        "multipart/form-data; boundary=rm-test",
        upload,
    )
    .await
    .unwrap();
    let paths: Vec<String> = serde_json::from_str(body(&reply)).unwrap();
    assert_eq!(paths.len(), 1);
    let input = json!({"data":[{"path":paths[0], "meta":{"_type":"gradio.FileData"}}]}).to_string();
    let reply = request(
        &run,
        &execution,
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
    let events = request(&run, &execution, "GET", &result_path, "", "")
        .await
        .unwrap();
    assert!(events.contains("text/event-stream"), "{events}");
    let first = events.find("started verified upload").expect(&events);
    let last = events.find("finished verified upload").expect(&events);
    assert!(
        first < last && events.contains("event: complete"),
        "{events}"
    );
    assert_eq!(run.stop().await.unwrap(), WorkloadExit::Stopped);
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);
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
