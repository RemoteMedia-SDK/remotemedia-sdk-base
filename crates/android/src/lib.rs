use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;
use log::{error, info, warn};
use remotemedia_core::{
    executor::SelectedRuntime,
    loadable::factory::LoadableNodeBundle,
    nodes::schema::create_builtin_schema_registry,
    transport::PipelineExecutor,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;

static APP_FILES_DIR: OnceLock<String> = OnceLock::new();
static EXECUTORS: LazyLock<Mutex<HashMap<jlong, Box<ExecutorRegistry>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct ExecutorRegistry {
    _runtime: SelectedRuntime,
    _executor: PipelineExecutor,
    _plugins: Vec<LoadableNodeBundle>,
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_initLogger(
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

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeSetAppFilesDir(
    mut env: JNIEnv,
    _class: JClass,
    files_dir: JString,
) {
    let dir: String = match env.get_string(&files_dir) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get files_dir string: {}", e);
            return;
        }
    };
    match APP_FILES_DIR.set(dir.clone()) {
        Ok(_) => info!("App files directory set to: {}", dir),
        Err(_) => error!("App files directory already set to {}, ignoring new value: {}", APP_FILES_DIR.get().unwrap_or(&"<unknown>".to_string()), dir),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeCreateExecutor(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    info!("Creating client-side pipeline executor for IN_PROCESS troubleshooting");

    let _plugins = load_android_loadable_plugins();

    let runtime = SelectedRuntime::CPython;

    let executor = match PipelineExecutor::new() {
        Ok(e) => e,
        Err(e) => {
            error!("Failed to create executor: {}", e);
            return 0;
        }
    };

    let registry = Box::new(ExecutorRegistry {
        _runtime: runtime,
        _executor: executor,
        _plugins,
    });

    let raw = Box::into_raw(registry) as jlong;
    EXECUTORS
        .lock()
        .unwrap()
        .insert(raw, unsafe { Box::from_raw(raw as *mut ExecutorRegistry) });
    raw
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
        (
            "LiteRT-LM",
            "/data/data/com.remotemedia.inprocess/files/liblitert_lm_loadable_plugin.so",
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
            }
        }
    }
    bundles
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeDestroyExecutor(
    _env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
) {
    if executor_ptr == 0 {
        return;
    }
    EXECUTORS.lock().unwrap().remove(&executor_ptr);
    info!("Destroyed pipeline executor");
}

/// Create a streaming session from a manifest
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeCreateSession(
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

    // Check if we are running in Microdroid mode
    let fd = VSOCK_FD.swap(-1, Ordering::SeqCst);
    if fd != -1 {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let session_id = uuid::Uuid::new_v4().to_string();
        let participant_id = "host-app".to_string();

        let (input_tx, input_rx) = mpsc::channel(256);
        let (output_tx, output_rx) = mpsc::channel(256);

        let connect_result = rt.block_on(async {
            use tower::service_fn;

            let std_stream = unsafe { StdUnixStream::from_raw_fd(fd) };
            std_stream.set_nonblocking(true).unwrap();

            let connector = service_fn(move |_| {
                let std_clone = std_stream.try_clone().unwrap();
                async move {
                    UnixStream::from_std(std_clone)
                }
            });

            let channel = tonic::transport::Endpoint::from_static("http://localhost")
                .connect_with_connector(connector)
                .await?;

            let mut client = RunnerControlClient::new(channel);

            // 1. Register Session
            client.register_session(RegisterSessionRequest {
                session_id: session_id.clone(),
                sub_manifest_json: pipeline_str.clone().into_bytes(),
                edges: vec![],
                session_metadata: std::collections::HashMap::new(),
                sdk_version: "0.1.0".to_string(),
            }).await?;

            // 2. Register Participant
            client.register_participant(RegisterParticipantRequest {
                session_id: session_id.clone(),
                participant_id: participant_id.clone(),
                participant_type: ParticipantType::Grpc as i32,
            }).await?;

            // 3. Send Participant Data stream
            let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_rx);
            let response = client.send_participant_data(input_stream).await?;
            let mut output_stream = response.into_inner();

            // Spawn receiver task
            let tx = output_tx.clone();
            let sid = session_id.clone();
            let pid = participant_id.clone();
            tokio::spawn(async move {
                while let Some(msg) = output_stream.message().await.ok().flatten() {
                    if msg.session_id != sid || msg.participant_id != pid {
                        continue;
                    }
                    let runtime_data: RuntimeData = match serde_json::from_slice(&msg.transport_data) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Failed to deserialize output: {}", e);
                            continue;
                        }
                    };
                    let kind = match &runtime_data {
                        RuntimeData::Audio { .. } => "audio",
                        RuntimeData::Video { .. } => "video",
                        _ => "data",
                    };
                    if tx.send(AndroidOutput {
                        source: kind.to_string(),
                        data: runtime_data,
                    }).await.is_err() {
                        break;
                    }
                }
                info!("Microdroid vsock output receiver stopped for {}", sid);
            });

            Ok::<(), anyhow::Error>(())
        });

        match connect_result {
            Ok(_) => {
                info!("Microdroid VM session registered successfully");
                let boxed = Box::new(AndroidSession {
                    rt,
                    input: AndroidSessionInput::Microdroid {
                        tx: input_tx,
                        session_id,
                        participant_id,
                    },
                    session: Mutex::new(None),
                    output_rx: AsyncMutex::new(output_rx),
                });
                return Box::into_raw(boxed) as jlong;
            }
            Err(e) => {
                error!("Failed to initialize Microdroid VM session: {}", e);
                return 0;
            }
        }
    }

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
                input: AndroidSessionInput::InProcess(input),
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

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeTestPythonNode(
    _env: JNIEnv,
    _class: JClass,
) -> jstring {
    let output = serde_json::json!({
        "error": "In-process Python execution is disabled in this build"
    })
    .to_string();
    _env.new_string(output).unwrap().into_raw()
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeSendInputText(
    _env: JNIEnv,
    _class: JClass,
    _session_ptr: jlong,
    _text: JString,
) -> jboolean {
    jni::sys::JNI_TRUE
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeSendInputAudio(
    _env: JNIEnv,
    _class: JClass,
    _session_ptr: jlong,
    _pcm_data: jni::objects::JByteArray,
    _sample_rate: jint,
    _channels: jint,
) -> jboolean {
    jni::sys::JNI_TRUE
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeRecvOutput(
    _env: JNIEnv,
    _class: JClass,
    _session_ptr: jlong,
) -> jstring {
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeCloseSession(
    _env: JNIEnv,
    _class: JClass,
    _session_ptr: jlong,
) {
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeGetAvailableNodes(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
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
            "parameters": {
                "model_path": "gemma-4-E2B-it.litertlm",
                "backend": "cpu",
                "model_sources": {
                    "files": [
                        {
                            "path": "gemma-4-E2B-it.litertlm",
                            "source": "huggingface",
                            "filename": "gemma-4-E2B-it.litertlm",
                            "required": true
                        }
                    ]
                }
            }
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

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeGetNodeSchema(
    mut env: JNIEnv,
    _class: JClass,
    node_type: JString,
) -> jstring {
    let node_type: String = env.get_string(&node_type).unwrap().into();
    let registry = create_builtin_schema_registry();
    if let Some(schema) = registry.get(&node_type) {
        let json_str = serde_json::to_string(&schema).unwrap_or_default();
        env.new_string(json_str).unwrap().into_raw()
    } else {
        env.new_string("{}").unwrap().into_raw()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeGetHermesProfileData(
    _env: JNIEnv,
    _class: JClass,
) -> jstring {
    let output = serde_json::json!({
        "error": "In-process Python execution is disabled in this build"
    })
    .to_string();
    _env.new_string(output).unwrap().into_raw()
}
