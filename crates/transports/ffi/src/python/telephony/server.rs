//! Python TelephonyServer implementation
//!
//! Implements a native SIP/RTP telephony gateway wrapper in PyO3 that runs
//! async loops and links calls dynamically to RemoteMedia pipelines.

use super::config::TelephonyServerConfig;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_async_runtimes::tokio::future_into_py;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::{self, Manifest};
use remotemedia_core::transport::{PipelineExecutor, StreamSession};
use remotemedia_telephony::codec::CodecMap;
use remotemedia_telephony::rtp::{RtpMediaSession, RtpOutboundMediaSession};
use remotemedia_telephony::sdp::NegotiatedAudio;
use remotemedia_telephony::session::ParticipantRole;
use remotemedia_telephony::{parse_request, AudioCodec, SipMethod, TelephonyTransport};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Active call session containing its pipeline stream session and RTP media state
struct ActiveSession {
    _call_id: String,
    _peer: SocketAddr,
    stream: Box<dyn StreamSession>,
    rtp_socket: Arc<tokio::net::UdpSocket>,
    _local_rtp_addr: SocketAddr,
    remote_rtp_addr: SocketAddr,
    negotiated_audio: NegotiatedAudio,
    _codec_map: CodecMap,
    _leg_id: String,
    inbound_media: RtpMediaSession,
    outbound_media: RtpOutboundMediaSession,
}

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
                                                                    // Build media processor and active call session state
                                                                    let inbound_media = RtpMediaSession::new(
                                                                        call_id.clone(),
                                                                        accepted.leg_id.clone(),
                                                                        ParticipantRole::User,
                                                                        accepted.codec_map.clone(),
                                                                        4, // Jitter target packets
                                                                    );

                                                                    let outbound_media = RtpOutboundMediaSession::new(
                                                                        accepted.negotiated_audio.codec,
                                                                        accepted.negotiated_audio.payload_type,
                                                                        0xABCDEF01, // random SSRC
                                                                        1,
                                                                        0,
                                                                        accepted.negotiated_audio.ptime_ms,
                                                                    ).unwrap_or_else(|_| {
                                                                        RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 0xABCDEF01, 1, 0, 20)
                                                                            .expect("SSRC build fallback failed")
                                                                    });

                                                                    let rtp_socket = Arc::new(accepted.rtp_socket);
                                                                    let local_rtp_addr = accepted.local_rtp_addr;
                                                                    let remote_rtp_addr = accepted.remote_rtp_addr;
                                                                    let negotiated_audio = accepted.negotiated_audio;
                                                                    let codec_map = accepted.codec_map;
                                                                    let leg_id = accepted.leg_id;

                                                                    let session = ActiveSession {
                                                                        _call_id: call_id.clone(),
                                                                        _peer: peer,
                                                                        stream: accepted.stream,
                                                                        rtp_socket,
                                                                        _local_rtp_addr: local_rtp_addr,
                                                                        remote_rtp_addr,
                                                                        negotiated_audio,
                                                                        _codec_map: codec_map,
                                                                        _leg_id: leg_id,
                                                                        inbound_media,
                                                                        outbound_media,
                                                                    };

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
                                                                        // Build media processor and active call session state
                                                                        let inbound_media = RtpMediaSession::new(
                                                                            call_id.clone(),
                                                                            accepted.leg_id.clone(),
                                                                            ParticipantRole::User,
                                                                            accepted.codec_map.clone(),
                                                                            4,
                                                                        );

                                                                        let outbound_media = RtpOutboundMediaSession::new(
                                                                            accepted.negotiated_audio.codec,
                                                                            accepted.negotiated_audio.payload_type,
                                                                            0xABCDEF01,
                                                                            1,
                                                                            0,
                                                                            accepted.negotiated_audio.ptime_ms,
                                                                        ).unwrap_or_else(|_| {
                                                                            RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 0xABCDEF01, 1, 0, 20)
                                                                                .expect("SSRC build fallback failed")
                                                                        });

                                                                        let rtp_socket = Arc::new(accepted.rtp_socket);
                                                                        let local_rtp_addr = accepted.local_rtp_addr;
                                                                        let remote_rtp_addr = accepted.remote_rtp_addr;
                                                                        let negotiated_audio = accepted.negotiated_audio;
                                                                        let codec_map = accepted.codec_map;
                                                                        let leg_id = accepted.leg_id;

                                                                        let session = ActiveSession {
                                                                            _call_id: call_id.clone(),
                                                                            _peer: peer,
                                                                            stream: accepted.stream,
                                                                            rtp_socket,
                                                                            _local_rtp_addr: local_rtp_addr,
                                                                            remote_rtp_addr,
                                                                            negotiated_audio,
                                                                            _codec_map: codec_map,
                                                                            _leg_id: leg_id,
                                                                            inbound_media,
                                                                            outbound_media,
                                                                        };

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

fn spawn_media_loop(
    call_id: String,
    session: ActiveSession,
    active_sessions: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let active_sessions_clone = active_sessions.clone();
    tokio::spawn(async move {
        let mut session = session;
        let mut buf = [0u8; 1500];
        let mut pending_samples: Vec<f32> = Vec::new();

        let mut latched = false;
        let mut remote_rtp_addr = session.remote_rtp_addr;

        // NAT Punching: Send 5 initial silence frames to remote_rtp_addr to open firewall/NAT bindings
        let target_rate = session.negotiated_audio.clock_rate_hz;
        let samples_per_frame = (target_rate / 1000) * u32::from(session.negotiated_audio.ptime_ms);
        let silence_frame = vec![0.0f32; samples_per_frame as usize];
        info!(
            "RTP NAT Punching: Sending 5 initial silence frames to remote {} to punch NAT hole...",
            remote_rtp_addr
        );
        for _ in 0..5 {
            if let Ok(rtp_packet) = session.outbound_media.packetize_audio_frame(&silence_frame) {
                let rtp_bytes = rtp_packet.write();
                let _ = session
                    .rtp_socket
                    .send_to(&rtp_bytes, remote_rtp_addr)
                    .await;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }

        loop {
            let mut read_ok = false;
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Graceful shutdown signal received for Call-ID: {}", call_id);
                    break;
                }
                recv_res = session.rtp_socket.recv_from(&mut buf) => {
                    match recv_res {
                        Ok((mut len, mut src)) => {
                            let mut packet_buf = buf.to_vec();
                            loop {
                                if !latched {
                                    info!("RTP Latching: Latched Call-ID: {} to actual remote RTP source: {}", call_id, src);
                                    remote_rtp_addr = src;
                                    latched = true;
                                }

                                if src.ip() == remote_rtp_addr.ip() {
                                    if let Ok(frames) = session.inbound_media.receive_datagram(&packet_buf[..len]) {
                                        for frame in frames {
                                            if session.stream.send_input(frame).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }

                                // Try to receive next pending packet non-blocking to prevent queue backup
                                let mut temp_buf = [0u8; 1500];
                                match session.rtp_socket.try_recv_from(&mut temp_buf) {
                                    Ok((next_len, next_src)) => {
                                        len = next_len;
                                        src = next_src;
                                        packet_buf = temp_buf.to_vec();
                                    }
                                    Err(_) => {
                                        break;
                                    }
                                }
                            }
                            read_ok = true;
                        }
                        Err(e) => {
                            warn!("RTP read socket failed for call {}: {}", call_id, e);
                        }
                    }
                }
                output_res = session.stream.recv_output() => {
                    match output_res {
                        Ok(Some(output_data)) => {
                            if let RuntimeData::Audio {
                                samples,
                                sample_rate,
                                channels,
                                ..
                            } = &output_data.data
                            {
                                let samples_slice = samples.as_slice();
                                let mono_samples = if *channels > 1 && !samples_slice.is_empty() {
                                    let mut out = Vec::with_capacity(samples_slice.len() / *channels as usize);
                                    for i in (0..samples_slice.len()).step_by(*channels as usize) {
                                        let chunk = &samples_slice[i..i.min(i + *channels as usize)];
                                        let sum: f32 = chunk.iter().sum();
                                        out.push(sum / chunk.len() as f32);
                                    }
                                    out
                                } else {
                                    samples_slice.to_vec()
                                };

                                let target_rate = session.negotiated_audio.clock_rate_hz;
                                let resampled = if *sample_rate != target_rate {
                                    let ratio = *sample_rate as f64 / target_rate as f64;
                                    let mut out = Vec::new();
                                    let mut pos = 0.0f64;
                                    for _ in 0..(mono_samples.len() as f64 / ratio) as usize {
                                        let idx = pos as usize;
                                        if idx < mono_samples.len() {
                                            out.push(mono_samples[idx]);
                                        }
                                        pos += ratio;
                                    }
                                    out
                                } else {
                                    mono_samples
                                };

                                let samples_per_frame = (target_rate / 1000) * u32::from(session.negotiated_audio.ptime_ms);
                                let samples_per_frame = samples_per_frame as usize;
                                pending_samples.extend(resampled);

                                while pending_samples.len() >= samples_per_frame {
                                    let frame: Vec<f32> = pending_samples.drain(..samples_per_frame).collect();
                                    if let Ok(packet) = session.outbound_media.packetize_audio_frame(&frame) {
                                        let _ = session.rtp_socket.send_to(&packet.write(), remote_rtp_addr).await;
                                    }
                                }
                            }
                            read_ok = true;
                        }
                        Ok(None) | Err(_) => {
                            break;
                        }
                    }
                }
            }

            if !read_ok {
                break;
            }
        }

        // Close stream on exit
        let _ = session.stream.close().await;

        // Final registry cleanup
        let mut active = active_sessions_clone.lock().await;
        active.remove(&call_id);
        info!(
            "Telephony session media loop finished for Call-ID: {}",
            call_id
        );
    });
}
