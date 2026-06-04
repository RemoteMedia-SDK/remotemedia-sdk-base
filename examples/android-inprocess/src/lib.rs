use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;
use log::{error, info};
use remotemedia_core::{
    executor::SelectedRuntime,
    manifest::Manifest,
    transport::{PipelineExecutor, TransportData, SessionHandle},
    data::{RuntimeData, AudioSamples},
};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

/// Initialize the Android logger
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeInitLogger(
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
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeCreateExecutor(
    mut _env: JNIEnv,
    _class: JClass,
) -> jlong {
    info!("Creating pipeline executor with in-process Python");

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

    // Box and leak the executor to get a raw pointer we can pass to Java
    // In production, use a proper handle map
    let boxed = Box::new((runtime, executor));
    Box::into_raw(boxed) as jlong
}

/// Execute a simple pipeline with in-process Python
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeRunPipeline(
    mut env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
    pipeline_json: JString,
) -> jstring {
    let executor_ptr = executor_ptr as *mut (SelectedRuntime, PipelineExecutor);
    if executor_ptr.is_null() {
        return env.new_string("Error: executor not initialized").unwrap().into_raw();
    }

    // Get the pipeline JSON from Java
    let pipeline_str: String = match env.get_string(&pipeline_json) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get pipeline string: {}", e);
            return env.new_string(&format!("Error: {}", e)).unwrap().into_raw();
        }
    };

    let (_runtime, executor) = unsafe { &mut *executor_ptr };

    // Parse the manifest
    let manifest: Manifest = match serde_json::from_str(&pipeline_str) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse manifest: {}", e);
            return env.new_string(&format!("Parse error: {}", e)).unwrap().into_raw();
        }
    };

    // Create a simple test input
    let input = RuntimeData::Text("Hello from Android!".to_string());
    let transport_data = TransportData::new(input);

    // Execute pipeline
    let manifest = Arc::new(manifest);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(async {
        executor.execute_unary(manifest, transport_data).await
    });

    match result {
        Ok(output) => {
            let output_json = serde_json::to_string(&output.data).unwrap_or_default();
            env.new_string(output_json).unwrap().into_raw()
        }
        Err(e) => {
            error!("Pipeline execution failed: {}", e);
            env.new_string(&format!("Execution error: {}", e)).unwrap().into_raw()
        }
    }
}

/// Clean up the executor
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeDestroyExecutor(
    _env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
) {
    let executor_ptr = executor_ptr as *mut (SelectedRuntime, PipelineExecutor);
    if !executor_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(executor_ptr);
        }
        info!("Executor destroyed");
    }
}

/// Test in-process Python node directly
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeTestPythonNode(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    // This tests the PythonNodeHandle directly
    use remotemedia_plugin_sdk::PythonNodeHandle;
    use pyo3::prelude::*;

    let result = Python::attach(|py| -> Result<serde_json::Value, anyhow::Error> {
        let code = c"
class EchoNode:
    def initialize(self, config):
        return {'status': 'initialized'}

    def process(self, input_data):
        return {'echo': input_data.get('text', 'no input')}

    def finalize(self):
        return {'status': 'finalized'}
";

        // Compile the code into sys.modules as a dynamic module
        let dict = pyo3::types::PyDict::new(py);
        py.run(code, Some(&dict), None)
            .map_err(|e| anyhow::anyhow!("Failed to run code: {:?}", e))?;
        
        let sys = py.import("sys")
            .map_err(|e| anyhow::anyhow!("Failed to import sys: {:?}", e))?;
        let modules_any = sys.getattr("modules")
            .map_err(|e| anyhow::anyhow!("Failed to get modules attribute: {:?}", e))?;
        let modules = modules_any.cast::<pyo3::types::PyDict>()
            .map_err(|e| anyhow::anyhow!("Failed to cast modules: {:?}", e))?;
        
        let types = py.import("types")
            .map_err(|e| anyhow::anyhow!("Failed to import types: {:?}", e))?;
        let module = types.getattr("ModuleType")
            .map_err(|e| anyhow::anyhow!("Failed to get ModuleType: {:?}", e))?
            .call1(("test_echo",))
            .map_err(|e| anyhow::anyhow!("Failed to create ModuleType: {:?}", e))?;
        
        let echo_node_class = dict.get_item("EchoNode")
            .map_err(|e| anyhow::anyhow!("Failed to get EchoNode from dict: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("EchoNode not found in dict"))?;
            
        module.setattr("EchoNode", echo_node_class)
            .map_err(|e| anyhow::anyhow!("Failed to set EchoNode attribute: {:?}", e))?;
            
        modules.set_item("test_echo", module)
            .map_err(|e| anyhow::anyhow!("Failed to set test_echo in modules: {:?}", e))?;

        // Now load the handle from sys.modules
        let handle = PythonNodeHandle::load("test_echo", "EchoNode")
            .map_err(|e| anyhow::anyhow!("Failed to load plugin: {:?}", e))?;
        let config = std::collections::HashMap::new();
        handle.initialize(&config)
            .map_err(|e| anyhow::anyhow!("Failed to initialize plugin: {:?}", e))?;

        let input = RuntimeData::Text("Hello from Android PyO3!".to_string());
        let output = handle.process(&input)
            .map_err(|e| anyhow::anyhow!("Failed to process plugin: {:?}", e))?;
        handle.finalize()
            .map_err(|e| anyhow::anyhow!("Failed to finalize plugin: {:?}", e))?;

        match output {
            RuntimeData::Json(v) => Ok(v),
            RuntimeData::Text(s) => Ok(serde_json::json!({"echo": s})),
            _ => Err(anyhow::anyhow!("Unexpected output format: {:?}", output)),
        }
    });

    match result {
        Ok(output) => {
            let json = serde_json::to_string(&output).unwrap_or_default();
            env.new_string(json).unwrap().into_raw()
        }
        Err(e) => {
            error!("Python node test failed: {}", e);
            env.new_string(&format!("Python test error: {}", e)).unwrap().into_raw()
        }
    }
}

/// Create a streaming session from a manifest
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeCreateSession(
    mut env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
    pipeline_json: JString,
) -> jlong {
    let executor_ptr = executor_ptr as *mut (SelectedRuntime, PipelineExecutor);
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

    let (_runtime, executor) = unsafe { &mut *executor_ptr };

    // Parse the manifest
    let manifest: Manifest = match serde_json::from_str(&pipeline_str) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse manifest: {}", e);
            return 0;
        }
    };

    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(async {
        executor.create_session(Arc::new(manifest)).await
    });

    match result {
        Ok(session_handle) => {
            info!("Session created successfully: {}", session_handle.session_id);
            // Box and leak the session handle
            let boxed = Box::new((rt, session_handle));
            Box::into_raw(boxed) as jlong
        }
        Err(e) => {
            error!("Failed to create session: {}", e);
            0
        }
    }
}

/// Send a text input to the session
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeSendInputText(
    mut env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
    text: JString,
) -> jboolean {
    let session_ptr = session_ptr as *mut (tokio::runtime::Runtime, SessionHandle);
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

    let (rt, session) = unsafe { &mut *session_ptr };
    let input = RuntimeData::Text(input_text);
    let transport_data = TransportData::new(input);

    let result = rt.block_on(async {
        session.send_input(transport_data).await
    });

    if let Err(e) = result {
        error!("Failed to send input text: {}", e);
        jni::sys::JNI_FALSE
    } else {
        jni::sys::JNI_TRUE
    }
}

/// Send audio samples (PCM 16-bit) to the session
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeSendInputAudio(
    env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
    pcm_data: jni::objects::JByteArray,
    sample_rate: jint,
    channels: jint,
) -> jboolean {
    let session_ptr = session_ptr as *mut (tokio::runtime::Runtime, SessionHandle);
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

    let (rt, session) = unsafe { &mut *session_ptr };
    let audio = AudioSamples::Vec(samples_f32);
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

    let result = rt.block_on(async {
        session.send_input(transport_data).await
    });

    if let Err(e) = result {
        error!("Failed to send audio input: {}", e);
        jni::sys::JNI_FALSE
    } else {
        jni::sys::JNI_TRUE
    }
}

/// Receive output from the session (blocks until output is available or channel is closed)
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeRecvOutput(
    env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) -> jstring {
    let session_ptr = session_ptr as *mut (tokio::runtime::Runtime, SessionHandle);
    if session_ptr.is_null() {
        return env.new_string("Error: Session not initialized").unwrap().into_raw();
    }

    let (rt, session) = unsafe { &mut *session_ptr };

    let result = rt.block_on(async {
        session.recv_output().await
    });

    match result {
        Ok(Some(output)) => {
            let output_json = serde_json::to_string(&output.data).unwrap_or_default();
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
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeCloseSession(
    _env: JNIEnv,
    _class: JClass,
    session_ptr: jlong,
) {
    let session_ptr = session_ptr as *mut (tokio::runtime::Runtime, SessionHandle);
    if !session_ptr.is_null() {
        unsafe {
            let boxed = Box::from_raw(session_ptr);
            let (rt, mut session) = *boxed;
            rt.block_on(async {
                let _ = session.close().await;
            });
        }
        info!("Session closed and destroyed");
    }
}

/// Node information for UI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub description: String,
    pub category: String,
    pub input_types: Vec<String>,
    pub output_types: Vec<String>,
    pub parameters: std::collections::HashMap<String, String>,
}

/// Get available nodes for UI
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_inprocess_MainActivity_nativeGetAvailableNodes(
    _env: JNIEnv,
    _class: JClass,
) -> jstring {
    // Return hardcoded list of available nodes for now
    // In production, this would query the node registry
    let nodes = vec![
        NodeInfo {
            name: "SileroVADNode".to_string(),
            description: "Voice Activity Detection using Silero ONNX".to_string(),
            category: "VAD".to_string(),
            input_types: vec!["Audio".to_string()],
            output_types: vec!["AudioSamples".to_string()],
            parameters: [
                ("threshold".to_string(), "0.5".to_string()),
                ("min_speech_duration_ms".to_string(), "250".to_string()),
                ("min_silence_duration_ms".to_string(), "1000".to_string()),
            ].into(),
        },
        NodeInfo {
            name: "WhisperSTTNode".to_string(),
            description: "Speech-to-Text using Whisper (Candle)".to_string(),
            category: "STT".to_string(),
            input_types: vec!["AudioSamples".to_string()],
            output_types: vec!["Text".to_string()],
            parameters: [
                ("model_size".to_string(), "tiny".to_string()),
                ("language".to_string(), "auto".to_string()),
                ("beam_size".to_string(), "1".to_string()),
            ].into(),
        },
        NodeInfo {
            name: "Phi3LLMNode".to_string(),
            description: "LLM using Phi-3-mini (GGUF)".to_string(),
            category: "LLM".to_string(),
            input_types: vec!["Text".to_string()],
            output_types: vec!["Text".to_string()],
            parameters: [
                ("max_tokens".to_string(), "150".to_string()),
                ("temperature".to_string(), "0.7".to_string()),
                ("top_p".to_string(), "0.9".to_string()),
                ("stream".to_string(), "true".to_string()),
            ].into(),
        },
        NodeInfo {
            name: "KokoroTTSNode".to_string(),
            description: "Text-to-Speech using Kokoro (ONNX)".to_string(),
            category: "TTS".to_string(),
            input_types: vec!["Text".to_string()],
            output_types: vec!["Audio".to_string()],
            parameters: [
                ("voice".to_string(), "af_bella".to_string()),
                ("speed".to_string(), "1.0".to_string()),
                ("stream".to_string(), "true".to_string()),
            ].into(),
        },
        NodeInfo {
            name: "LiteRtLmGenerationNode".to_string(),
            description: "LLM using LiteRT-LM / Gemma (Native)".to_string(),
            category: "LLM".to_string(),
            input_types: vec!["Text".to_string()],
            output_types: vec!["Text".to_string()],
            parameters: [
                ("model_path".to_string(), "assets://models/llm/gemma/".to_string()),
                ("max_tokens".to_string(), "150".to_string()),
                ("temperature".to_string(), "0.7".to_string()),
            ].into(),
        },
    ];
    
    let json = serde_json::to_string(&nodes).unwrap_or_else(|_| "[]".to_string());
    match _env.new_string(json) {
        Ok(s) => s.into_raw(),
        Err(_) => _env.new_string("[]").unwrap().into_raw(),
    }
}