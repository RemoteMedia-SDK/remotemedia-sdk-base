use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;
use log::{error, info};
use pyo3::prelude::*;
use remotemedia_core::{
    data::{AudioSamples, RuntimeData},
    executor::SelectedRuntime,
    loadable::factory::{wrap_ffi_factory, LoadableNodeBundle},
    manifest::Manifest,
    transport::{
        session_control::ControlAddress, ClientOutputReceivers, PipelineExecutor, SessionHandle,
        SessionInputSender, TransportData,
    },
};
use remotemedia_python_nodes::{
    register_default_python_nodes, register_python_node, NodeProvider, PythonNodeConfig,
    PythonNodesProvider,
};
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

const ANDROID_APP_FILES_DIR: &str = "/data/data/com.remotemedia.inprocess/files";
const ANDROID_PYTHON_HOME: &str = "/data/data/com.remotemedia.inprocess/files/python/bundle";
const ANDROID_PYTHON_SRC: &str = "/data/data/com.remotemedia.inprocess/files/python/src";
static AUDIO_SEND_COUNT: AtomicU64 = AtomicU64::new(0);

type AndroidExecutor = (SelectedRuntime, PipelineExecutor, Vec<LoadableNodeBundle>);

struct AndroidSession {
    rt: tokio::runtime::Runtime,
    input: SessionInputSender,
    session: Mutex<Option<SessionHandle>>,
    output_rx: AsyncMutex<mpsc::Receiver<AndroidOutput>>,
}

#[derive(Debug, Serialize)]
struct AndroidOutput {
    source: String,
    data: RuntimeData,
}

fn register_android_inprocess_python_nodes() {
    // Android cannot use the multiprocess/iceoryx2 Python path. Re-register the
    // Python-backed node types used by this example as in-process PyO3 nodes.
    register_python_node(
        PythonNodeConfig::new("WhisperSTTNode")
            .with_python_class("remotemedia.nodes.android_inprocess.WhisperSTTNode")
            .with_description("Android in-process STT adapter")
            .with_category("stt")
            .accepts(["audio"])
            .produces(["text"])
            .with_inprocess(true),
    );

    register_python_node(
        PythonNodeConfig::new("DebugKokoroTTSNode")
            .with_python_class("remotemedia.nodes.android_inprocess.DebugKokoroTTSNode")
            .with_multi_output(true)
            .with_description("Android in-process debug TTS sine adapter")
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"])
            .with_inprocess(true),
    );

    register_python_node(
        PythonNodeConfig::new("VADNode")
            .with_python_class("remotemedia.nodes.android_inprocess.VADNode")
            .with_multi_output(true)
            .with_description("Android in-process VAD adapter")
            .with_category("vad")
            .accepts(["audio"])
            .produces(["vad"])
            .with_inprocess(true),
    );

    register_python_node(
        PythonNodeConfig::new("DataSinkNode")
            .with_python_class("remotemedia.nodes.android_inprocess.DataSinkNode")
            .with_description(
                "Sink node for sending output to external systems (Android AudioPlayer)",
            )
            .with_category("io")
            .accepts(["audio"])
            .produces(Vec::<String>::new())
            .with_inprocess(true),
    );
}

/// Initialize the Android logger
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_initLogger(
    _env: JNIEnv,
    _class: JClass,
) {
    android_logger::init_once(
        android_logger::Config::default()
            .with_max_level(log::LevelFilter::Debug)
            .with_tag("RemoteMedia"),
    );
    info!("RemoteMedia Android logger initialized");
}

/// Initialize the Python runtime and create a pipeline executor
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeCreateExecutor(
    mut _env: JNIEnv,
    _class: JClass,
) -> jlong {
    info!("Creating pipeline executor with in-process Python");
    configure_android_python_environment();

    // Register default Python nodes, then Android-only aliases/debug adapters.
    register_default_python_nodes();
    info!("Registered default Python nodes");
    register_android_inprocess_python_nodes();
    info!("Registered Android in-process Python nodes: WhisperSTTNode alias, DebugKokoroTTSNode, VADNode, DataSinkNode");

    let loadable_bundles = load_android_loadable_plugins();

    // Select in-process Python runtime (will default to in-process on Android)
    let runtime = SelectedRuntime::CPython;
    info!("Selected runtime: {:?}", runtime);

    // Create executor
    let executor = match PipelineExecutor::new() {
        Ok(e) => e,
        Err(e) => {
            error!("Failed to create executor: {}", e);
            return 0;
        }
    };

    {
        let registry = executor.registry();
        let mut registry = registry.blocking_write();
        PythonNodesProvider.register(&mut registry);
    }
    // PythonNodesProvider performs its own default registration on first use,
    // which can replace the Android overrides. Replay the Android overrides and
    // register the provider again so these factories win in this executor.
    register_android_inprocess_python_nodes();
    {
        let registry = executor.registry();
        let mut registry = registry.blocking_write();
        PythonNodesProvider.register(&mut registry);
    }
    info!("Registered Android in-process Python factories into executor registry");

    if !loadable_bundles.is_empty() {
        info!("Registering loadable plugin factories into executor registry");
        let registry = executor.registry();
        let mut registry = registry.blocking_write();
        for bundle in &loadable_bundles {
            bundle.register_into(&mut registry);
        }
    }
    {
        let registry = executor.registry();
        let mut registry = registry.blocking_write();
        registry.register(wrap_ffi_factory(
            litert_lm_loadable_plugin::LiteRtLmGenerationNodeFactory,
        ));
    }
    info!("Registered linked LiteRT-LM factory into executor registry");

    info!("Registered Android loadable plugin factories");

    // Box and leak the executor to get a raw pointer we can pass to Java
    // In production, use a proper handle map
    let boxed = Box::new((runtime, executor, loadable_bundles));
    Box::into_raw(boxed) as jlong
}

fn load_android_loadable_plugins() -> Vec<LoadableNodeBundle> {
    let plugin_paths = [
        (
            "Silero VAD",
            "/data/data/com.remotemedia.inprocess/files/libsilero_vad_loadable_plugin.so",
        ),
        (
            "Whisper LiteRT",
            "/data/data/com.remotemedia.inprocess/files/libwhisper_loadable_plugin.so",
        ),
        (
            "Misaki G2P",
            "/data/data/com.remotemedia.inprocess/files/libmisaki_g2p_plugin.so",
        ),
        (
            "Kokoro ONNX",
            "/data/data/com.remotemedia.inprocess/files/libkokoro_onnx_plugin.so",
        ),
    ];
    let mut bundles = Vec::new();
    for (label, path) in plugin_paths {
        let plugin_path = Path::new(path);
        info!("Loading {} plugin from: {:?}", label, plugin_path);
        match LoadableNodeBundle::load(plugin_path) {
            Ok(bundle) => {
                info!(
                    "Loaded {} plugin, factories: {:?}",
                    label,
                    bundle
                        .factories()
                        .iter()
                        .map(|f| f.node_type())
                        .collect::<Vec<_>>()
                );
                bundles.push(bundle);
            }
            Err(e) => {
                error!("Failed to load {} plugin: {}", label, e);
                info!("Continuing without {} plugin", label);
            }
        }
    }
    info!(
        "Android TTS backend diagnostics: production KokoroTTSNode is expected from libkokoro_onnx_plugin.so; Python debug adapter is registered only as DebugKokoroTTSNode"
    );
    bundles
}

/// Clean up the executor
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeDestroyExecutor(
    _env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
) {
    let executor_ptr = executor_ptr as *mut AndroidExecutor;
    if !executor_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(executor_ptr);
        }
        info!("Executor destroyed");
    }
}

/// Create a streaming session from a manifest
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeCreateSession(
    mut env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
    pipeline_json: JString,
) -> jlong {
    let executor_ptr = executor_ptr as *mut AndroidExecutor;
    if executor_ptr.is_null() {
        error!("Executor pointer is null");
        return 0;
    }

    // Get the pipeline JSON from Java
    let pipeline_str: String = match env.get_string(&pipeline_json) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get pipeline string: {}", e);
            return 0;
        }
    };

    let (_runtime, executor, loadable_bundles) = unsafe { &mut *executor_ptr };

    // Parse the manifest
    let manifest: Manifest = match serde_json::from_str(&pipeline_str) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse manifest: {}", e);
            return 0;
        }
    };

    log_manifest_diagnostics(&manifest);

    if !loadable_bundles.is_empty() {
        info!(
            "Executor has {} loadable plugin bundle(s) retained for session",
            loadable_bundles.len()
        );
    }
    let node_ids: Vec<String> = manifest.nodes.iter().map(|node| node.id.clone()).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(async { executor.create_session(Arc::new(manifest)).await });

    match result {
        Ok(mut session_handle) => {
            info!(
                "Session created successfully: {}",
                session_handle.session_id
            );
            let Some(input) = session_handle.input_sender() else {
                error!("Session did not expose an input sender");
                return 0;
            };
            let Some(output_receivers) = session_handle.take_output_receivers() else {
                error!("Session did not expose output receivers");
                return 0;
            };
            let (output_tx, output_rx) = mpsc::channel(256);
            spawn_android_output_drainers(
                &rt,
                session_handle.session_id.clone(),
                output_receivers,
                output_tx.clone(),
            );
            if let Some(control) = executor.control_bus().get(&session_handle.session_id) {
                spawn_android_node_tap_drainers(
                    &rt,
                    session_handle.session_id.clone(),
                    control,
                    &node_ids,
                    output_tx,
                );
            }
            spawn_android_runtime_heartbeat(&rt, session_handle.session_id.clone());
            // Box and leak the session handle
            let boxed = Box::new(AndroidSession {
                rt,
                input,
                session: Mutex::new(Some(session_handle)),
                output_rx: AsyncMutex::new(output_rx),
            });
            Box::into_raw(boxed) as jlong
        }
        Err(e) => {
            error!("Failed to create session: {}", e);
            0
        }
    }
}

fn spawn_android_output_drainers(
    rt: &tokio::runtime::Runtime,
    session_id: String,
    receivers: ClientOutputReceivers,
    output_tx: mpsc::Sender<AndroidOutput>,
) {
    let ClientOutputReceivers {
        audio_rx,
        video_rx,
        data_rx,
    } = receivers;

    for (kind, mut rx) in [("audio", audio_rx), ("video", video_rx), ("data", data_rx)] {
        let tx = output_tx.clone();
        let sid = session_id.clone();
        rt.spawn(async move {
            while let Some(output) = rx.recv().await {
                info!(
                    "Android output drainer received {} output for {}: {}",
                    kind,
                    sid,
                    describe_android_runtime_data(&output)
                );
                if tx
                    .send(AndroidOutput {
                        source: kind.to_string(),
                        data: output,
                    })
                    .await
                    .is_err()
                {
                    info!(
                        "Android output receiver dropped for {}; stopping {} drainer",
                        sid, kind
                    );
                    break;
                }
            }
            info!("Android {} output drainer stopped for {}", kind, sid);
        });
    }
}

fn spawn_android_node_tap_drainers(
    rt: &tokio::runtime::Runtime,
    session_id: String,
    control: Arc<remotemedia_core::transport::session_control::SessionControl>,
    node_ids: &[String],
    output_tx: mpsc::Sender<AndroidOutput>,
) {
    for node_id in ["vad", "stt", "llm"] {
        if !node_ids.iter().any(|id| id == node_id) {
            continue;
        }
        let mut rx = match control.subscribe(&ControlAddress::node_out(node_id)) {
            Ok(rx) => rx,
            Err(e) => {
                error!(
                    "Failed to subscribe Android Conversation tap for {} in {}: {}",
                    node_id, session_id, e
                );
                continue;
            }
        };
        let tx = output_tx.clone();
        let sid = session_id.clone();
        let source = node_id.to_string();
        rt.spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(output) => {
                        if source == "vad" && !matches!(output, RuntimeData::Json(_)) {
                            continue;
                        }
                        info!(
                            "Android Conversation tap received {} output for {}: {}",
                            source,
                            sid,
                            describe_android_runtime_data(&output)
                        );
                        if tx
                            .send(AndroidOutput {
                                source: source.clone(),
                                data: output,
                            })
                            .await
                            .is_err()
                        {
                            info!(
                                "Android output receiver dropped for {}; stopping {} tap drainer",
                                sid, source
                            );
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        info!(
                            "Android Conversation tap for {} in {} lagged by {} message(s)",
                            source, sid, skipped
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            info!("Android {} tap drainer stopped for {}", source, sid);
        });
    }
}

fn spawn_android_runtime_heartbeat(rt: &tokio::runtime::Runtime, session_id: String) {
    rt.spawn(async move {
        let mut ticks = 0u64;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            ticks += 1;
            info!(
                "Android session runtime heartbeat {} for {}",
                ticks, session_id
            );
        }
    });
}

fn describe_android_runtime_data(data: &RuntimeData) -> String {
    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            metadata,
            ..
        } => {
            let kokoro = metadata
                .as_ref()
                .and_then(|metadata| metadata.get("kokoro_onnx"))
                .map(|kokoro| format!(" metadata={kokoro}"))
                .unwrap_or_default();
            format!(
                "audio samples={} rate={}Hz channels={}{}",
                samples.len(),
                sample_rate,
                channels,
                kokoro
            )
        }
        RuntimeData::Video {
            pixel_data,
            width,
            height,
            ..
        } => format!("video {}x{} bytes={}", width, height, pixel_data.len()),
        RuntimeData::Text(text) => format!("text chars={}", text.chars().count()),
        RuntimeData::Json(value) => value
            .as_object()
            .map(|object| {
                format!(
                    "json keys=[{}]",
                    object.keys().take(8).cloned().collect::<Vec<_>>().join(",")
                )
            })
            .unwrap_or_else(|| "json value".to_string()),
        other => other.data_type().to_string(),
    }
}

/// Test Python initialization and import visibility for Android debugging.
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeTestPythonNode(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    configure_android_python_environment();

    let result = Python::attach(|py| -> PyResult<String> {
        let sys = py.import("sys")?;
        let version: String = sys.getattr("version")?.extract()?;
        let path = sys.getattr("path")?.repr()?.to_string();
        let numpy = py.import("numpy").map(|_| true).unwrap_or(false);
        let remotemedia = py.import("remotemedia").map(|_| true).unwrap_or(false);
        Ok(serde_json::json!({
            "python_version": version,
            "sys_path": path,
            "numpy_importable": numpy,
            "remotemedia_importable": remotemedia
        })
        .to_string())
    });

    let output = match result {
        Ok(value) => value,
        Err(e) => {
            error!("Python diagnostic failed: {:?}", e);
            serde_json::json!({
                "error": format!("{:?}", e)
            })
            .to_string()
        }
    };

    env.new_string(output).unwrap().into_raw()
}

/// Send text input to the session
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeSendInputText(
    mut env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
    text: JString,
) -> jboolean {
    let session_ptr = session_ptr as *mut AndroidSession;
    if session_ptr.is_null() {
        error!("Session pointer is null");
        return jni::sys::JNI_FALSE;
    }

    let input_text: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get input text: {}", e);
            return jni::sys::JNI_FALSE;
        }
    };

    let session = unsafe { &*session_ptr };
    let input = RuntimeData::Text(input_text);
    let transport_data = TransportData::new(input);

    let result = session
        .rt
        .block_on(async { session.input.send(transport_data).await });

    if let Err(e) = result {
        error!("Failed to send input text: {}", e);
        jni::sys::JNI_FALSE
    } else {
        jni::sys::JNI_TRUE
    }
}

/// Send audio samples (PCM 16-bit) to the session
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeSendInputAudio(
    env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
    pcm_data: jni::objects::JByteArray,
    sample_rate: jint,
    channels: jint,
) -> jboolean {
    let session_ptr = session_ptr as *mut AndroidSession;
    if session_ptr.is_null() {
        error!("Session pointer is null");
        return jni::sys::JNI_FALSE;
    }

    let pcm_bytes = match env.convert_byte_array(pcm_data) {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to get PCM byte array: {}", e);
            return jni::sys::JNI_FALSE;
        }
    };

    // Convert byte array to i16 samples, then normalize to f32 (-1.0 to 1.0)
    let samples_f32: Vec<f32> = pcm_bytes
        .chunks_exact(2)
        .map(|c| {
            let sample_i16 = i16::from_le_bytes([c[0], c[1]]);
            sample_i16 as f32 / 32768.0
        })
        .collect();

    let sample_count = samples_f32.len();
    let session = unsafe { &*session_ptr };
    let audio = AudioSamples::Vec(samples_f32);
    let sent_count = AUDIO_SEND_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if sent_count <= 3 || sent_count % 10 == 0 {
        info!(
            "Sending audio frame {} to session: samples={}, sample_rate={}, channels={}",
            sent_count, sample_count, sample_rate, channels
        );
    }
    let input = RuntimeData::Audio {
        samples: audio,
        sample_rate: sample_rate as u32,
        channels: channels as u32,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    let transport_data = TransportData::new(input);

    let result = session
        .rt
        .block_on(async { session.input.send(transport_data).await });

    if let Err(e) = result {
        error!("Failed to send audio input: {}", e);
        jni::sys::JNI_FALSE
    } else {
        jni::sys::JNI_TRUE
    }
}

/// Receive output from the session (blocks until output is available or channel is closed)
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeRecvOutput(
    env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) -> jstring {
    let session_ptr = session_ptr as *mut AndroidSession;
    if session_ptr.is_null() {
        return env
            .new_string("Error: Session not initialized")
            .unwrap()
            .into_raw();
    }

    let session = unsafe { &*session_ptr };

    let result = session.rt.block_on(async {
        let mut rx = session.output_rx.lock().await;
        Ok::<_, remotemedia_core::Error>(rx.recv().await)
    });

    match result {
        Ok(Some(output)) => {
            let output_json = serde_json::to_string(&output).unwrap_or_default();
            info!("Received session output: {} bytes", output_json.len());
            env.new_string(output_json).unwrap().into_raw()
        }
        Ok(None) => {
            env.new_string("").unwrap().into_raw() // End of stream
        }
        Err(e) => {
            error!("Failed to receive output: {}", e);
            env.new_string(&format!("Error: {}", e)).unwrap().into_raw()
        }
    }
}

/// Close and destroy a session
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeCloseSession(
    _env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) {
    let session_ptr = session_ptr as *mut AndroidSession;
    if !session_ptr.is_null() {
        unsafe {
            let boxed = Box::from_raw(session_ptr);
            let mut session_handle = boxed.session.lock().unwrap().take();
            boxed.rt.block_on(async {
                if let Some(ref mut session) = session_handle {
                    let _ = session.close().await;
                }
            });
        }
        info!("Session closed and destroyed");
    }
}

fn log_manifest_diagnostics(manifest: &Manifest) {
    info!(
        "Session manifest diagnostics: name='{}', nodes={}, plugins={}",
        manifest.metadata.name,
        manifest.nodes.len(),
        manifest.plugins.len()
    );

    for plugin in &manifest.plugins {
        info!("Manifest plugin spec: {:?}", plugin);
        match plugin {
            remotemedia_core::manifest::PluginSpec::Shorthand(path) => {
                log_path_metadata("plugin", path);
            }
            remotemedia_core::manifest::PluginSpec::Explicit(spec) => {
                if let Some(path) = spec.path.as_deref() {
                    log_path_metadata("plugin", path);
                }
            }
        }
    }

    for node in &manifest.nodes {
        info!(
            "Manifest node: id='{}', type='{}', streaming={}, params={}",
            node.id, node.node_type, node.is_streaming, node.params
        );

        if let Some(params) = node.params.as_object() {
            for key in [
                "model_path",
                "tokenizer_path",
                "config_path",
                "cache_dir",
                "litert_dispatch_lib_dir",
            ] {
                if let Some(path) = params.get(key).and_then(|v| v.as_str()) {
                    log_path_metadata(&format!("node '{}'.{}", node.id, key), path);
                }
            }
        }
    }
}

fn configure_android_python_environment() {
    let python_path = format!(
        "{home}/stdlib.zip:{home}/modules:{home}/site-packages:{src}",
        home = ANDROID_PYTHON_HOME,
        src = ANDROID_PYTHON_SRC
    );

    std::env::set_var("PYTHONHOME", ANDROID_PYTHON_HOME);
    std::env::set_var("PYTHONPATH", &python_path);
    std::env::set_var("PYTHONNOUSERSITE", "1");
    std::env::set_var("PYTHONDONTWRITEBYTECODE", "1");
    std::env::set_var("REMOTEMEDIA_NODE_TRACE", "1");
    if std::env::var_os("REMOTEMEDIA_NODE_TIMEOUT_MS").is_none() {
        std::env::set_var("REMOTEMEDIA_NODE_TIMEOUT_MS", "120000");
    }

    info!("Configured Android Python environment");
    log_path_metadata("app files dir", ANDROID_APP_FILES_DIR);
    log_path_metadata("PYTHONHOME", ANDROID_PYTHON_HOME);
    log_path_metadata(
        "PYTHONPATH stdlib.zip",
        &format!("{ANDROID_PYTHON_HOME}/stdlib.zip"),
    );
    log_path_metadata(
        "PYTHONPATH modules",
        &format!("{ANDROID_PYTHON_HOME}/modules"),
    );
    log_path_metadata(
        "PYTHONPATH site-packages",
        &format!("{ANDROID_PYTHON_HOME}/site-packages"),
    );
    log_path_metadata("PYTHONPATH remotemedia src", ANDROID_PYTHON_SRC);
}

fn log_path_metadata(label: &str, path: &str) {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            info!(
                "{} path exists: '{}', file={}, dir={}, len={} bytes",
                label,
                path,
                metadata.is_file(),
                metadata.is_dir(),
                metadata.len()
            );
        }
        Err(e) => {
            error!("{} path missing/unreadable: '{}': {}", label, path, e);
        }
    }
}

/// Get available nodes for UI
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeGetAvailableNodes(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    // Return hardcoded list of available nodes for now
    let nodes = vec![
        serde_json::json!({
            "name": "WhisperNode",
            "description": "Speech-to-text using LiteRT Whisper",
            "category": "STT",
            "input_types": ["audio"],
            "output_types": ["text"],
            "parameters": {
                "model": "tiny",
                "backend": "litert",
                "model_path": "models/whisper/whisper_tiny_30s_f32.tflite",
                "tokenizer_path": "models/whisper/tokenizer.json",
                "language": "en",
                "task": "transcribe"
            }
        }),
        serde_json::json!({
            "name": "KokoroTTSNode",
            "description": "Text-to-speech using Kokoro TTS",
            "category": "TTS",
            "input_types": ["text"],
            "output_types": ["audio"],
            "parameters": {"voice": "af_bella", "speed": 1.0}
        }),
        serde_json::json!({
            "name": "SileroVADNode",
            "description": "Voice Activity Detection using Silero ONNX",
            "category": "VAD",
            "input_types": ["audio"],
            "output_types": ["vad"],
            "parameters": {
                "model_path": "models/silero-vad/silero_vad.onnx",
                "threshold": 0.5,
                "sampling_rate": 16000
            }
        }),
        serde_json::json!({
            "name": "LiteRtLmGenerationNode",
            "description": "Gemma 4 Google LiteRT LLM",
            "category": "LLM",
            "input_types": ["text"],
            "output_types": ["text"],
            "parameters": {"model_path": "gemma-4-E2B-it.litertlm", "backend": "cpu"}
        }),
        serde_json::json!({
            "name": "DataSinkNode",
            "description": "Sink node for sending output to external systems",
            "category": "IO",
            "input_types": ["audio"],
            "output_types": [],
            "parameters": {}
        }),
    ];

    let json_str = serde_json::to_string(&nodes).unwrap_or_default();
    env.new_string(json_str).unwrap().into_raw()
}

/// Start streaming mode
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeStartStreaming(
    _env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
) -> jboolean {
    let executor_ptr = executor_ptr as *mut AndroidExecutor;
    if executor_ptr.is_null() {
        error!("Executor pointer is null");
        return jni::sys::JNI_FALSE;
    }

    // This function is a placeholder - actual streaming is done via session
    info!("Start streaming called");
    jni::sys::JNI_TRUE
}

/// Stop streaming
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_NativeInterface_nativeStopStreaming(
    _env: JNIEnv,
    _class: JClass,
    _executor_ptr: jlong,
) -> jboolean {
    info!("Stop streaming called");
    jni::sys::JNI_TRUE
}
