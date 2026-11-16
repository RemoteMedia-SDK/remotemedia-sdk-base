//! Python TelephonyServer implementation
//!
//! Implements a native SIP/RTP telephony gateway wrapper in PyO3 that runs
//! async loops and links calls dynamically to RemoteMedia pipelines.

use super::config::TelephonyServerConfig;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_async_runtimes::tokio::future_into_py;
use remotemedia_core::manifest::{self, Manifest};
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_telephony::media_loop::CallMedia;
use remotemedia_telephony::{parse_request, SipMethod, TelephonyTransport};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Native SIP/RTP Telephony Server for Python
///
/// Binds a SIP signaling UDP socket, accepts inbound calls from Twilio/SBCs,
/// and dynamically runs pipeline execution instances per call session.
#[pyclass]
pub struct TelephonyServer {
    _config: TelephonyServerConfig,
    manifest: Arc<Manifest>,
    transport: Arc<TelephonyTransport>,
    active_sessions: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    server_running: Arc<AtomicBool>,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<()>>>>,
    executor: Arc<PipelineExecutor>,
    #[cfg(feature = "grpc")]
    grpc_server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    #[cfg(feature = "grpc")]
    grpc_shutdown_flag: Arc<AtomicBool>,
}

#[pymethods]
impl TelephonyServer {
    /// Create a new Telephony gateway server
    ///
    /// Args:
    ///     config: TelephonyServerConfig instance
    ///
    /// Returns:
    ///     TelephonyServer instance
    #[staticmethod]
    fn create(py: Python<'_>, config: TelephonyServerConfig) -> PyResult<Py<TelephonyServer>> {
        let core_config = config.to_core_config()?;

        // Parse and validate pipeline manifest
        let manifest = manifest::parse(&config.manifest_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid manifest: {}", e))
        })?;
        let manifest = Arc::new(manifest);
        manifest::validate(&manifest).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Manifest validation failed: {}", e))
        })?;

        // Instantiate PipelineExecutor and TelephonyTransport
        let executor = Arc::new(PipelineExecutor::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to build PipelineExecutor: {}",
                e
            ))
        })?);
        let transport = Arc::new(
            TelephonyTransport::new(core_config, executor.clone()).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to build TelephonyTransport: {}",
                    e
                ))
            })?,
        );

        #[cfg(feature = "grpc")]
        let grpc_server_handle = Arc::new(Mutex::new(None));
        #[cfg(feature = "grpc")]
        let grpc_shutdown_flag = Arc::new(AtomicBool::new(false));

        Py::new(
            py,
            TelephonyServer {
                _config: config,
                manifest,
                transport,
                active_sessions: Arc::new(Mutex::new(HashMap::new())),
                server_running: Arc::new(AtomicBool::new(false)),
                server_handle: Arc::new(Mutex::new(None)),
                shutdown_tx: Arc::new(Mutex::new(None)),
                executor,
                #[cfg(feature = "grpc")]
                grpc_server_handle,
                #[cfg(feature = "grpc")]
                grpc_shutdown_flag,
            },
        )
    }

    /// Start the telephony signaling and media gateway loops
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.transport.clone();
        let manifest = self.manifest.clone();
        let active_sessions = self.active_sessions.clone();
        let running = self.server_running.clone();
        let handle_lock = self.server_handle.clone();
        let shutdown_lock = self.shutdown_tx.clone();
        let executor = self.executor.clone();
        let control_plane_port = self._config.control_plane_port;

        #[cfg(feature = "grpc")]
        let grpc_shutdown_flag = self.grpc_shutdown_flag.clone();
        #[cfg(feature = "grpc")]
        let grpc_server_handle = self.grpc_server_handle.clone();

        future_into_py(py, async move {
            if running.load(Ordering::SeqCst) {
                return Ok(());
            }

            // Bind the SIP listener socket
            let socket = transport.bind_sip_socket().await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to bind SIP socket: {}",
                    e
                ))
            })?;
            let local_addr = socket.local_addr().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to get bound address: {}",
                    e
                ))
            })?;
            info!(
                "Telephony Python Gateway listening on SIP UDP {}",
                local_addr
            );

            // Start gRPC control plane if control_plane_port is configured
            #[cfg(feature = "grpc")]
            {
                if let Some(port) = control_plane_port {
                    let mut grpc_config = remotemedia_grpc::ServiceConfig::default();
                    grpc_config.bind_address = format!("0.0.0.0:{}", port);

                    let server =
                        remotemedia_grpc::GrpcServer::new(grpc_config, executor).map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to construct gRPC server: {}",
                                e
                            ))
                        })?;

                    grpc_shutdown_flag.store(false, Ordering::SeqCst);
                    let shutdown_clone = grpc_shutdown_flag.clone();

                    info!("Starting gRPC Control Plane on 0.0.0.0:{}", port);
                    let handle = tokio::spawn(async move {
                        if let Err(e) = server.serve_with_shutdown_flag(shutdown_clone).await {
                            error!("gRPC server error: {}", e);
                        }
                    });

                    *grpc_server_handle.lock().await = Some(handle);
                }
            }

            let socket = Arc::new(socket);
            running.store(true, Ordering::SeqCst);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
            *shutdown_lock.lock().await = Some(tx);

            // Spawn the main SIP transaction listener thread in Tokio
            let running_clone = running.clone();
            let active_sessions_clone = active_sessions.clone();
            let handle = tokio::spawn(async move {
                let mut buf = vec![0_u8; transport.config().max_sip_datagram_bytes];

                loop {
                    tokio::select! {
                        _ = rx.recv() => {
                            info!("Graceful shutdown signal received by native telephony loop.");
                            break;
                        }
                        recv_res = socket.recv_from(&mut buf) => {
                            match recv_res {
                                Ok((len, peer)) => {
                                    let datagram = &buf[..len];
                                    match parse_request(datagram) {
                                        Ok(request) => {
                                            match request.method {
                                                SipMethod::Invite => {
                                                    let call_id = request.call_id().unwrap_or("unknown").to_string();
                                                    info!("SIP: Inbound INVITE from {} (Call-ID: {})", peer, call_id);

                                                    let transport_clone = Arc::clone(&transport);
                                                    let socket_clone = Arc::clone(&socket);
                                                    let manifest_clone = Arc::clone(&manifest);
                                                    let active_sessions_clone = Arc::clone(&active_sessions_clone);
                                                    let invite_bytes = datagram.to_vec();

                                                    tokio::spawn(async move {
                                                        match transport_clone.accept_inbound_call(&invite_bytes, manifest_clone).await {
                                                            Ok(accepted) => {
                                                                info!("Call negotiated successfully. Emitting SIP 200 OK for Call-ID: {}", call_id);
                                                                if let Err(e) = socket_clone.send_to(&accepted.response.bytes, peer).await {
                                                                    error!("Failed to write 200 OK SIP to {}: {}", peer, e);
                                                                } else {
                                                                    // The call's media: caller RTP into the pipeline, pipeline audio back as
                                                                    // paced RTP with `audio_released` reports and flushes
                                                                    // (`remotemedia_telephony::media_loop`).
                                                                    let session = CallMedia::from_accepted(accepted);

                                                                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                                                                    {
                                                                        let mut active = active_sessions_clone.lock().await;
                                                                        active.insert(call_id.clone(), shutdown_tx);
                                                                    }
                                                                    spawn_media_loop(call_id, session, active_sessions_clone, shutdown_rx);
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!("Pipeline instantiation failed for inbound call from {}: {}", peer, e);
                                                                let via = request.header("via").unwrap_or("");
                                                                let from = request.header("from").unwrap_or("");
                                                                let to = request.header("to").unwrap_or("");
                                                                let cseq = request.header("cseq").unwrap_or("");
                                                                let err_response = format!(
                                                                    "SIP/2.0 500 Internal Server Error\r\n\
                                                                    Via: {via}\r\n\
                                                                    From: {from}\r\n\
                                                                    To: {to}\r\n\
                                                                    Call-ID: {call_id}\r\n\
                                                                    CSeq: {cseq}\r\n\
                                                                    Content-Length: 0\r\n\r\n"
                                                                );
                                                                let _ = socket_clone.send_to(err_response.as_bytes(), peer).await;
                                                            }
                                                        }
                                                    });
                                                }
                                                SipMethod::Bye | SipMethod::Cancel => {
                                                    let call_id = request.call_id().unwrap_or("unknown").to_string();
                                                    info!("SIP: Terminating Call-ID: {}", call_id);

                                                    let mut active = active_sessions_clone.lock().await;
                                                    if let Some(shutdown_tx) = active.remove(&call_id) {
                                                        let _ = shutdown_tx.send(());
                                                    }

                                                    if let Ok(Some(response)) = transport.handle_sip_datagram_from(datagram, peer) {
                                                        let _ = socket.send_to(&response.bytes, peer).await;
                                                    }
                                                }
                                                SipMethod::Options => {
                                                    if let Ok(Some(response)) = transport.handle_sip_datagram_from(datagram, peer) {
                                                        let _ = socket.send_to(&response.bytes, peer).await;
                                                    }
                                                }
                                                SipMethod::Ack => {
                                                    let _ = transport.handle_sip_datagram_from(datagram, peer);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Malformed SIP request from {}: {}", peer, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    if running_clone.load(Ordering::SeqCst) {
                                        error!("Error on SIP UDP socket: {}", e);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            *handle_lock.lock().await = Some(handle);
            Ok(())
        })
    }

    /// Shutdown the server and tear down all active call legs gracefully
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let running = self.server_running.clone();
        let handle_lock = self.server_handle.clone();
        let shutdown_lock = self.shutdown_tx.clone();
        let active_sessions = self.active_sessions.clone();
        let transport = self.transport.clone();

        #[cfg(feature = "grpc")]
        let grpc_shutdown_flag = self.grpc_shutdown_flag.clone();
        #[cfg(feature = "grpc")]
        let grpc_server_handle = self.grpc_server_handle.clone();

        future_into_py(py, async move {
            running.store(false, Ordering::SeqCst);

            // Shutdown gRPC server if it was started
            #[cfg(feature = "grpc")]
            {
                grpc_shutdown_flag.store(true, Ordering::SeqCst);
                if let Some(h) = grpc_server_handle.lock().await.take() {
                    let _ = h.await;
                    info!("gRPC Control Plane stopped.");
                }
            }

            if let Some(tx) = shutdown_lock.lock().await.take() {
                let _ = tx.send(()).await;
            }

            if let Some(handle) = handle_lock.lock().await.take() {
                let _ = handle.await;
            }

            // Tear down active sessions and close sockets
            let _ = transport.shutdown().await;
            let mut active = active_sessions.lock().await;
            for (call_id, shutdown_tx) in active.drain() {
                info!(
                    "Closing pipeline execution for Call-ID: {} during shutdown",
                    call_id
                );
                let _ = shutdown_tx.send(());
            }

            Ok(())
        })
    }

    /// Context manager support: async with server
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let server = slf.clone_ref(py);
        future_into_py(py, async move {
            let (transport, manifest, active_sessions, running, handle_lock, shutdown_lock) =
                Python::attach(|py| {
                    let s = server.bind(py);
                    (
                        s.borrow().transport.clone(),
                        s.borrow().manifest.clone(),
                        s.borrow().active_sessions.clone(),
                        s.borrow().server_running.clone(),
                        s.borrow().server_handle.clone(),
                        s.borrow().shutdown_tx.clone(),
                    )
                });

            if !running.load(Ordering::SeqCst) {
                // Bind the SIP listener socket
                let socket = transport.bind_sip_socket().await.map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to bind SIP socket: {}",
                        e
                    ))
                })?;
                let local_addr = socket.local_addr().map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to get bound address: {}",
                        e
                    ))
                })?;
                info!(
                    "Telephony Python Gateway listening on SIP UDP {}",
                    local_addr
                );

                let socket = Arc::new(socket);
                running.store(true, Ordering::SeqCst);

                let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                *shutdown_lock.lock().await = Some(tx);

                // Spawn the main SIP transaction listener thread in Tokio
                let running_clone = running.clone();
                let active_sessions_clone = active_sessions.clone();
                let handle = tokio::spawn(async move {
                    let mut buf = vec![0_u8; transport.config().max_sip_datagram_bytes];

                    loop {
                        tokio::select! {
                            _ = rx.recv() => {
                                info!("Graceful shutdown signal received by native telephony loop.");
                                break;
                            }
                            recv_res = socket.recv_from(&mut buf) => {
                                match recv_res {
                                    Ok((len, peer)) => {
                                        let datagram = &buf[..len];
                                        match parse_request(datagram) {
                                            Ok(request) => {
                                                match request.method {
                                                    SipMethod::Invite => {
                                                        let call_id = request.call_id().unwrap_or("unknown").to_string();
                                                        info!("SIP: Inbound INVITE from {} (Call-ID: {})", peer, call_id);

                                                        let transport_clone = Arc::clone(&transport);
                                                        let socket_clone = Arc::clone(&socket);
                                                        let manifest_clone = Arc::clone(&manifest);
                                                        let active_sessions_clone = Arc::clone(&active_sessions_clone);
                                                        let invite_bytes = datagram.to_vec();

                                                        tokio::spawn(async move {
                                                            match transport_clone.accept_inbound_call(&invite_bytes, manifest_clone).await {
                                                                Ok(accepted) => {
                                                                    info!("Call negotiated successfully. Emitting SIP 200 OK for Call-ID: {}", call_id);
                                                                    if let Err(e) = socket_clone.send_to(&accepted.response.bytes, peer).await {
                                                                        error!("Failed to write 200 OK SIP to {}: {}", peer, e);
                                                                    } else {
                                                                        // The call's media: caller RTP into the pipeline, pipeline audio back as
                                                                        // paced RTP with `audio_released` reports and flushes
                                                                        // (`remotemedia_telephony::media_loop`).
                                                                        let session = CallMedia::from_accepted(accepted);

                                                                        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                                                                        {
                                                                            let mut active = active_sessions_clone.lock().await;
                                                                            active.insert(call_id.clone(), shutdown_tx);
                                                                        }
                                                                        spawn_media_loop(call_id, session, active_sessions_clone, shutdown_rx);
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!("Pipeline instantiation failed for inbound call from {}: {}", peer, e);
                                                                    let via = request.header("via").unwrap_or("");
                                                                    let from = request.header("from").unwrap_or("");
                                                                    let to = request.header("to").unwrap_or("");
                                                                    let cseq = request.header("cseq").unwrap_or("");
                                                                    let err_response = format!(
                                                                        "SIP/2.0 500 Internal Server Error\r\n\
                                                                        Via: {via}\r\n\
                                                                        From: {from}\r\n\
                                                                        To: {to}\r\n\
                                                                        Call-ID: {call_id}\r\n\
                                                                        CSeq: {cseq}\r\n\
                                                                        Content-Length: 0\r\n\r\n"
                                                                    );
                                                                    let _ = socket_clone.send_to(err_response.as_bytes(), peer).await;
                                                                }
                                                            }
                                                        });
                                                    }
                                                    SipMethod::Bye | SipMethod::Cancel => {
                                                        let call_id = request.call_id().unwrap_or("unknown").to_string();
                                                        info!("SIP: Terminating Call-ID: {}", call_id);

                                                        let mut active = active_sessions_clone.lock().await;
                                                        if let Some(shutdown_tx) = active.remove(&call_id) {
                                                            let _ = shutdown_tx.send(());
                                                        }

                                                        if let Ok(Some(response)) = transport.handle_sip_datagram_from(datagram, peer) {
                                                            let _ = socket.send_to(&response.bytes, peer).await;
                                                        }
                                                    }
                                                    SipMethod::Options => {
                                                        if let Ok(Some(response)) = transport.handle_sip_datagram_from(datagram, peer) {
                                                            let _ = socket.send_to(&response.bytes, peer).await;
                                                        }
                                                    }
                                                    SipMethod::Ack => {
                                                        let _ = transport.handle_sip_datagram_from(datagram, peer);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Malformed SIP request from {}: {}", peer, e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if running_clone.load(Ordering::SeqCst) {
                                            error!("Error on SIP UDP socket: {}", e);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                *handle_lock.lock().await = Some(handle);
            }

            Ok(server)
        })
    }

    /// Context manager support: cleanup on exit
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_val: Option<Bound<'py, PyAny>>,
        _exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let running = self.server_running.clone();
        let handle_lock = self.server_handle.clone();
        let shutdown_lock = self.shutdown_tx.clone();
        let active_sessions = self.active_sessions.clone();
        let transport = self.transport.clone();

        future_into_py(py, async move {
            running.store(false, Ordering::SeqCst);

            if let Some(tx) = shutdown_lock.lock().await.take() {
                let _ = tx.send(()).await;
            }

            if let Some(handle) = handle_lock.lock().await.take() {
                let _ = handle.await;
            }

            // Tear down active sessions and close sockets
            let _ = transport.shutdown().await;
            let mut active = active_sessions.lock().await;
            for (call_id, shutdown_tx) in active.drain() {
                info!(
                    "Closing pipeline execution for Call-ID: {} during shutdown",
                    call_id
                );
                let _ = shutdown_tx.send(());
            }

            Ok(false) // Don't suppress exceptions
        })
    }
}

/// Run the call's media loop (`remotemedia_telephony::media_loop`) until the
/// call is shut down or its pipeline session ends.
fn spawn_media_loop(
    call_id: String,
    media: CallMedia,
    active_sessions: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        // Sends one RTP packet per frame duration, closes the stream at the end.
        media.run(shutdown_rx).await;

        // Final registry cleanup
        let mut active = active_sessions.lock().await;
        active.remove(&call_id);
        info!(
            "Telephony session media loop finished for Call-ID: {}",
            call_id
        );
    });
}
