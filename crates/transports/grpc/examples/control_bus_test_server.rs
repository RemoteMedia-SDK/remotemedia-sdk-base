//! Minimal test-helper binary for the Python control-bus integration
//! tests.
//!
//! Starts a gRPC server bound to an ephemeral port, creates a live
//! pipeline session with a single `CalculatorNode`, and prints one line
//! to stdout:
//!
//!     READY <port> <session_id>
//!
//! The server runs until killed. Python drives the data plane by
//! continuing to call `SessionHandle::send_input` via a second RPC
//! client (not needed for this test) or by using `publish` on the
//! control bus.
//!
//! Intended to be spawned by `clients/python/tests/test_control_bus_grpc.py`.

use remotemedia_core::manifest::{
    Connection, Manifest, ManifestMetadata, ManifestPythonEnv, NodeManifest,
};
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::control::ControlServiceImpl;
use remotemedia_grpc::generated::{
    pipeline_control_server::PipelineControlServer,
    pipeline_execution_service_server::PipelineExecutionServiceServer,
    streaming_pipeline_service_server::StreamingPipelineServiceServer,
};
use remotemedia_grpc::{
    metrics::ServiceMetrics, ExecutionServiceImpl, ServiceConfig, StreamingServiceImpl,
};
use std::io::Write;
use std::sync::Arc;
use tonic::transport::Server;

// Force-link `remotemedia-python-nodes` so its `inventory::submit!` macro
// invocations for `PythonNodesProvider` are pulled in. Without an
// explicit reference here, the linker can drop the entire crate — which
// is how `LFM2TextNode` / `KokoroTTSNode` / ... go missing from the
// default streaming registry at runtime.
#[allow(unused_imports)]
use remotemedia_python_nodes as _python_nodes_link;

fn calc_manifest() -> Manifest {
    Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "control-bus-test-pipeline".to_string(),
            ..Default::default()
        },
        nodes: vec![NodeManifest {
            id: "calc".to_string(),
            node_type: "CalculatorNode".to_string(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        connections: Vec::<Connection>::new(),
        ..Default::default()
    }
}

/// LFM2 text pipeline. Node runs in a Python multiprocess worker that
/// the Rust server spawns automatically when the session initializes.
fn lfm2_manifest() -> Manifest {
    Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "lfm2-control-bus-test-pipeline".to_string(),
            ..Default::default()
        },
        nodes: vec![NodeManifest {
            id: "lfm".to_string(),
            node_type: "LFM2TextNode".to_string(),
            params: serde_json::json!({
                "hf_repo": "LiquidAI/LFM2-350M",
                "max_new_tokens": 80,
                "do_sample": false,
            }),
            ..Default::default()
        }],
        connections: Vec::<Connection>::new(),
        ..Default::default()
    }
}

/// LFM2-Audio pipeline with server-side Whisper transcription taps.
///
/// Topology:
///
///     ┌───────────┐          ┌─────────┐          ┌──────────┐
///     │ stt_in.in │   (pub)  │  audio  │  audio   │ stt_out  │   text
///     │ (client)  │ ──────►  │ (LFM2)  │ ───────► │ (Whisper)│ ────►
///     └───────────┘          └─────────┘          └──────────┘
///           │                      ▲                    │
///           │ audio                │ audio              │
///           ▼                      │ (same bytes,       ▼
///     ┌──────────┐                 │  second publish)   text
///     │ stt_in   │                 │
///     │(Whisper) │ ──► text        │
///     └──────────┘                 │
///
/// Clients publish user audio twice — once to `audio.in` (which the
/// LFM2 node consumes) and once to `stt_in.in` (which the input
/// Whisper node consumes). The LFM2 output fans out server-side to
/// `stt_out`. All three sinks are subscribable via the control bus:
///
///     subscribe("audio.out")    → interleaved text + audio reply
///     subscribe("stt_in.out")   → transcript of what the user said
///     subscribe("stt_out.out")  → transcript of what LFM2 spoke back
fn lfm2_audio_manifest() -> Manifest {
    let whisper_params = serde_json::json!({
        "model_id": "openai/whisper-tiny.en",
        "language": "en",
    });

    // Backend selector. Set `LFM2_AUDIO_BACKEND=mlx` to run the
    // Apple-Silicon-native MLX build (mlx-community/LFM2.5-Audio-1.5B-4bit),
    // which runs on Metal without torch or CUDA. Default is "torch"
    // (liquid-audio + torch/torchaudio).
    let use_mlx = matches!(
        std::env::var("LFM2_AUDIO_BACKEND").as_deref(),
        Ok("mlx") | Ok("MLX") | Ok("apple"),
    );

    let (audio_node_type, audio_params, audio_deps) = if use_mlx {
        (
            "LFM2AudioMlxNode".to_string(),
            serde_json::json!({
                "hf_repo": "mlx-community/LFM2.5-Audio-1.5B-4bit",
                "max_new_tokens": 512,
                "sample_rate": 24000,
                "text_only": false,
            }),
            vec![
                // MLX port — pulls mlx, mlx-lm, mimi detokenizer.
                "mlx-audio>=0.1".to_string(),
                "numpy>=1.24".to_string(),
            ],
        )
    } else {
        (
            "LFM2AudioNode".to_string(),
            serde_json::json!({
                "hf_repo": "LiquidAI/LFM2.5-Audio-1.5B",
                "max_new_tokens": 512,
                "sample_rate": 24000,
                "text_only": false,
            }),
            vec![
                "liquid-audio>=0.1".to_string(),
                // liquid_audio 1.1.0 imports a transformers-4.54-era
                // private symbol (`Lfm2HybridConvCache`) that 5.x
                // removed — keep on the 4.x line.
                "transformers>=4.54.0,<5.0".to_string(),
                "torch>=2.1".to_string(),
                "torchaudio>=2.1".to_string(),
                "accelerate>=0.33".to_string(),
            ],
        )
    };

    let whisper_deps = vec![
        "transformers>=4.40.0".to_string(),
        "torch>=2.1".to_string(),
        "accelerate>=0.33".to_string(),
    ];

    Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "lfm2-audio-control-bus-test-pipeline".to_string(),
            ..Default::default()
        },
        nodes: vec![
            NodeManifest {
                id: "audio".to_string(),
                node_type: audio_node_type,
                params: audio_params,
                python_deps: Some(audio_deps),
                ..Default::default()
            },
            NodeManifest {
                id: "stt_in".to_string(),
                node_type: "WhisperSTTNode".to_string(),
                params: whisper_params.clone(),
                python_deps: Some(whisper_deps.clone()),
                ..Default::default()
            },
            NodeManifest {
                id: "stt_out".to_string(),
                node_type: "WhisperSTTNode".to_string(),
                params: whisper_params,
                python_deps: Some(whisper_deps),
                ..Default::default()
            },
        ],
        connections: vec![Connection {
            from: "audio".to_string(),
            to: "stt_out".to_string(),
            ..Default::default()
        }],
        // LFM2-Audio (via `liquid-audio`) requires Python >= 3.12. Pin
        // the managed-venv interpreter so `uv` provisions a 3.12 venv
        // instead of defaulting to whatever `python3` happens to be on
        // PATH (3.11 on this host). `uv` auto-downloads the interpreter
        // if it's missing. This is a manifest-scope setting so all three
        // nodes use the same Python; pick the max of everyone's floors.
        python_env: Some(ManifestPythonEnv {
            python_version: Some("3.12".to_string()),
            scope: None,
            extra_deps: Vec::new(),
        }),
        ..Default::default()
    }
}

fn select_manifest() -> Manifest {
    match std::env::var("TEST_SESSION_KIND").as_deref() {
        Ok("lfm2") => lfm2_manifest(),
        Ok("lfm2audio") | Ok("lfm2_audio") => lfm2_audio_manifest(),
        _ => calc_manifest(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Honor RUST_LOG so test runners can see what happens during session
    // bringup (node registry lookups, Python multiprocess spawn, ...).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let executor = Arc::new(PipelineExecutor::new()?);

    // Pre-create a session so the Python test can attach without needing
    // to drive StreamPipeline itself. Manifest selected by
    // `TEST_SESSION_KIND` env var (default: calc, valid: lfm2).
    let session = executor.create_session(Arc::new(select_manifest())).await?;
    let session_id = session.session_id.clone();
    // Leak the session handle so the router task keeps running for the
    // test's lifetime. Dropping it would close the input channel and
    // tear down the session.
    std::mem::forget(session);

    // Build services.
    let config = ServiceConfig::default();
    let metrics = Arc::new(ServiceMetrics::with_default_registry()?);
    let execution = ExecutionServiceImpl::new(
        config.auth.clone(),
        config.limits.clone(),
        metrics.clone(),
        executor.clone(),
    );
    let streaming =
        StreamingServiceImpl::new(config.auth, config.limits, metrics, executor.clone());
    let control = ControlServiceImpl::new(executor.control_bus());

    // Signal ready to the Python test runner.
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "READY {port} {session_id}")?;
    stdout.flush()?;
    drop(stdout);

    Server::builder()
        .add_service(PipelineExecutionServiceServer::new(execution))
        .add_service(StreamingPipelineServiceServer::new(streaming))
        .add_service(PipelineControlServer::new(control))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;

    Ok(())
}
