//! FFI bindings for RemoteMedia pipelines (Python and Node.js)
//!
//! This crate provides language bindings for the RemoteMedia core,
//! enabling both Python and Node.js applications to execute media processing
//! pipelines with Rust acceleration.
//!
//! # Features
//!
//! - `python` (default): Enable Python bindings via PyO3
//! - `napi`: Enable Node.js bindings via napi-rs
//! - `webrtc`: Enable WebRTC core types and signaling service wrapper
//! - `napi-webrtc`: Enable Node.js WebRTC bindings (requires `napi` + `webrtc`)
//! - `python-webrtc`: Enable Python WebRTC bindings (requires `python` + `webrtc`)
//!
//! # Architecture
//!
//! ## Shared modules
//! - **marshal.rs**: Data serialization (requires `python` feature)
//!
//! ## Python-specific (`python` feature)
//! - **api.rs**: Python FFI functions
//! - **numpy_bridge.rs**: Zero-copy numpy array integration
//! - **instance_handler.rs**: Python Node instance execution
//! - **python/webrtc/**: WebRTC server bindings (requires `python-webrtc`)
//!
//! ## Node.js-specific (`napi` feature)
//! - **napi/mod.rs**: Node.js module entry point
//! - **napi/subscriber.rs**: Zero-copy IPC subscriber
//! - **napi/publisher.rs**: Zero-copy IPC publisher
//! - **napi/sample.rs**: Sample lifecycle management
//! - **napi/webrtc/**: WebRTC server bindings (requires `napi-webrtc`)
//!
//! # Usage (Python)
//!
//! ```python
//! import asyncio
//! from remotemedia.runtime import execute_pipeline
//!
//! async def main():
//!     manifest = '{"version": "v1", ...}'
//!     results = await execute_pipeline(manifest)
//!     print(results)
//!
//! asyncio.run(main())
//! ```
//!
//! # Usage (Node.js)
//!
//! ```javascript
//! const { createSession } = require('@matbee/remotemedia-native');
//!
//! const session = createSession({ id: 'my-session' });
//! const channel = session.channel('audio_input');
//! const subscriber = channel.createSubscriber();
//!
//! subscriber.onData((sample) => {
//!     const data = sample.toRuntimeData();
//!     console.log('Received:', data.type);
//!     sample.release();
//! });
//! ```

#![warn(clippy::all)]

// Python-specific modules (only compiled with `python` feature)
#[cfg(feature = "python")]
mod api;
#[cfg(feature = "python")]
pub mod control_session;
#[cfg(feature = "python")]
pub mod inline_python_node;
#[cfg(feature = "python")]
pub mod instance_handler;
#[cfg(feature = "python")]
pub mod marshal;
#[cfg(feature = "python")]
mod numpy_bridge;
#[cfg(feature = "python")]
pub mod plugins;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub mod streaming_session;

// Node.js-specific modules (only compiled with `napi` feature)
#[cfg(feature = "napi")]
pub mod napi;

// WebRTC shared module (only compiled with `webrtc` feature)
#[cfg(feature = "webrtc")]
pub mod webrtc;

// Python module entry point
#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Python module for RemoteMedia Rust Runtime
///
/// Provides async pipeline execution with Rust acceleration
/// Installed as remotemedia.runtime
#[cfg(feature = "python")]
#[pymodule]
#[pyo3(name = "runtime")]
fn remotemedia_ffi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing on module load
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    // Add FFI functions from api module
    m.add_function(wrap_pyfunction!(api::execute_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(api::execute_pipeline_with_input, m)?)?;
    m.add_function(wrap_pyfunction!(api::execute_pipeline_with_instances, m)?)?;
    m.add_function(wrap_pyfunction!(api::get_runtime_version, m)?)?;
    m.add_function(wrap_pyfunction!(api::is_available, m)?)?;

    // Late node registration (custom Python nodes)
    m.add_function(wrap_pyfunction!(api::register_inline_node_class, m)?)?;
    m.add_function(wrap_pyfunction!(api::register_python_node_type, m)?)?;
    m.add_function(wrap_pyfunction!(api::unregister_node_type, m)?)?;
    m.add_function(wrap_pyfunction!(api::list_registered_node_types, m)?)?;

    // Streaming sessions (long-lived, bidirectional)
    m.add_function(wrap_pyfunction!(
        streaming_session::create_streaming_session,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        streaming_session::create_shared_streaming_session,
        m
    )?)?;
    m.add_class::<streaming_session::PyStreamingSession>()?;

    // In-proc control bus (PyAttachedSession + PySubscription + PyInterceptSession)
    m.add_class::<control_session::PyAttachedSession>()?;
    m.add_class::<control_session::PySubscription>()?;
    m.add_class::<control_session::PyInterceptSession>()?;
    {
        let py = m.py();
        m.add(
            "SessionNotFoundError",
            py.get_type::<control_session::SessionNotFoundError>(),
        )?;
        m.add(
            "ControlAddressError",
            py.get_type::<control_session::ControlAddressError>(),
        )?;
    }

    // Loadable cdylib plugins (third-party .so/.dll/.dylib nodes)
    m.add_function(wrap_pyfunction!(plugins::load_plugin, m)?)?;
    m.add_function(wrap_pyfunction!(plugins::list_loaded_plugins, m)?)?;

    // WebRTC bindings (requires python-webrtc feature)
    #[cfg(feature = "python-webrtc")]
    {
        use python::webrtc::config::{
            PeerCapabilities, PeerInfo, SessionInfo, TurnServer, WebRtcServerConfig,
        };
        use python::webrtc::events::{
            DataReceivedEvent, ErrorEvent, PeerConnectedEvent, PeerDisconnectedEvent,
            PipelineOutputEvent, SessionEvent,
        };
        use python::webrtc::server::WebRtcServer;
        use python::webrtc::session::WebRtcSession;

        let webrtc_module = pyo3::types::PyModule::new(m.py(), "webrtc")?;
        webrtc_module.add_class::<WebRtcServer>()?;
        webrtc_module.add_class::<WebRtcSession>()?;
        webrtc_module.add_class::<WebRtcServerConfig>()?;
        webrtc_module.add_class::<TurnServer>()?;
        webrtc_module.add_class::<PeerCapabilities>()?;
        webrtc_module.add_class::<PeerInfo>()?;
        webrtc_module.add_class::<SessionInfo>()?;
        webrtc_module.add_class::<PeerConnectedEvent>()?;
        webrtc_module.add_class::<PeerDisconnectedEvent>()?;
        webrtc_module.add_class::<PipelineOutputEvent>()?;
        webrtc_module.add_class::<DataReceivedEvent>()?;
        webrtc_module.add_class::<ErrorEvent>()?;
        webrtc_module.add_class::<SessionEvent>()?;
        m.add_submodule(&webrtc_module)?;
    }

    // Telephony bindings (requires python-telephony feature)
    #[cfg(feature = "python-telephony")]
    {
        use python::telephony::config::TelephonyServerConfig;
        use python::telephony::server::TelephonyServer;

        let telephony_module = pyo3::types::PyModule::new(m.py(), "telephony")?;
        telephony_module.add_class::<TelephonyServer>()?;
        telephony_module.add_class::<TelephonyServerConfig>()?;
        m.add_submodule(&telephony_module)?;
    }

    // gRPC bindings (requires grpc feature)
    #[cfg(feature = "grpc")]
    {
        m.add_class::<python::grpc_server::PyGrpcServer>()?;
    }

    // Add version as module constant
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
