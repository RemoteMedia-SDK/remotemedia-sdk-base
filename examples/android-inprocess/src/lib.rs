use jni::objects::{JClass, JString, JValueGen};
use jni::sys::{jboolean, jint, jlong, jstring};
use jni::JNIEnv;
use log::{error, info};
use remotemedia_core::{
    executor::{PipelineExecutor, RuntimeSelector},
    manifest::Manifest,
    transport::TransportData,
    Data, RuntimeData,
};
use serde_json::json;
use std::sync::Arc;

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
    env: JNIEnv,
    _class: JClass,
) -> jlong {
    info!("Creating pipeline executor with in-process Python");

    // Select in-process Python runtime (will default to in-process on Android)
    let runtime = match RuntimeSelector::auto_detect_runtime() {
        Ok(r) => {
            info!("Selected runtime: {:?}", r);
            r
        }
        Err(e) => {
            error!("Failed to detect runtime: {}", e);
            return 0;
        }
    };

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
    env: JNIEnv,
    _class: JClass,
    executor_ptr: jlong,
    pipeline_json: JString,
) -> jstring {
    let executor_ptr = executor_ptr as *mut (RuntimeSelector::SelectedRuntime, PipelineExecutor);
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
    let executor_ptr = executor_ptr as *mut (RuntimeSelector::SelectedRuntime, PipelineExecutor);
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
    use remotemedia_plugin_sdk::inprocess_python::{PythonNodeHandle, PythonConfig};

    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(async {
        // Try to load a simple echo plugin
        let config = PythonConfig {
            module_name: "test_echo".to_string(),
            class_name: "EchoNode".to_string(),
            source_code: Some(r#"
class EchoNode:
    def initialize(self, config):
        return {'status': 'initialized'}

    def process(self, input_data):
        return {'echo': input_data.get('text', 'no input')}

    def finalize(self):
        return {'status': 'finalized'}
"#.to_string()),
            init_params: serde_json::json!({}),
        };

        let mut handle = PythonNodeHandle::new(config);
        handle.load().await?;
        handle.initialize().await?;
        let output = handle.process(serde_json::json!({"text": "Hello from Android PyO3!"})).await?;
        handle.finalize().await?;

        Ok::<_, anyhow::Error>(output)
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