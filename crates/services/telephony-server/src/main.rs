//! Telephony Server Binary
//!
//! Exposes a turnkey telephony server that loads a RemoteMedia manifest,
//! binds a SIP listener socket, and bridges inbound calls to streaming pipeline sessions.

use clap::Parser;
use remotemedia_core::manifest;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_telephony::media_loop::CallMedia;
use remotemedia_telephony::{
    parse_request, SipMethod, TelephonyTransport, TelephonyTransportConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// RemoteMedia Telephony Server
///
/// Run a SIP/RTP telephony gateway linked to a streaming RemoteMedia pipeline.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to TOML configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Path to pipeline manifest JSON
    #[arg(short, long)]
    manifest: Option<PathBuf>,

    /// SIP bind address (e.g. 0.0.0.0:5060)
    #[arg(long)]
    sip_bind_address: Option<String>,

    /// Advertised media address for SDP answers (e.g. public VM IP)
    #[arg(long)]
    advertised_media_address: Option<String>,

    /// RTP port range (e.g. 16384-32767)
    #[arg(long)]
    rtp_port_range: Option<String>,

    /// Allowed SIP peers (comma-separated IP addresses or domains)
    #[arg(long, value_delimiter = ',')]
    allowed_peers: Option<Vec<String>>,

    /// Access mode: allowlist (deny by default) or denylist (allow by default)
    #[arg(long, default_value = "allowlist")]
    access_mode: String,

    /// Blocked SIP peers (comma-separated IP addresses or domains)
    #[arg(long, value_delimiter = ',')]
    blocked_peers: Option<Vec<String>>,

    /// Rate limit: max requests per window
    #[arg(long)]
    rate_limit_max: Option<u32>,

    /// Rate limit: window duration in seconds
    #[arg(long)]
    rate_limit_window: Option<u32>,

    /// Rate limit: ban duration in seconds
    #[arg(long)]
    rate_limit_ban_duration: Option<u32>,

    /// Enable SIPREC mirrored-call ingestion
    #[arg(long)]
    enable_siprec: Option<bool>,

    /// Maximum concurrent active calls
    #[arg(long)]
    max_active_calls: Option<u32>,

    /// Future TLS/SRTP placeholder: certificate path
    #[arg(long)]
    tls_cert_path: Option<String>,

    /// Future TLS/SRTP placeholder: private key path
    #[arg(long)]
    tls_key_path: Option<String>,

    /// Future TLS/SRTP placeholder: enable SRTP
    #[arg(long)]
    enable_srtp: Option<bool>,
}

/// Server configuration structure including future-compatible TLS/SRTP placeholders.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// Optional path to pipeline manifest file
    pub manifest_path: Option<String>,

    /// Nested telephony transport configuration
    #[serde(default)]
    pub telephony: TelephonyTransportConfig,

    /// Future TLS/SRTP certificate path
    pub tls_cert_path: Option<String>,

    /// Future TLS/SRTP private key path
    pub tls_key_path: Option<String>,

    /// Future SRTP enable flag
    pub enable_srtp: Option<bool>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            manifest_path: None,
            telephony: TelephonyTransportConfig::default(),
            tls_cert_path: None,
            tls_key_path: None,
            enable_srtp: None,
        }
    }
}

/// The running calls by Call-ID: the sender that stops a call's media loop.
type ActiveCalls = Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>;

/// Spawn the call's media loop (`remotemedia_telephony::media_loop`): caller
/// RTP into the pipeline, pipeline audio back as paced RTP, with the call's
/// `audio_released` reports and flushes. It runs until the call is removed
/// from `active_calls` (BYE, CANCEL, shutdown) or its pipeline session ends.
async fn spawn_media_loop(call_id: String, media: CallMedia, active_calls: ActiveCalls) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    active_calls.lock().await.insert(call_id.clone(), stop);
    tokio::spawn(async move {
        media.run(stopped).await;
        active_calls.lock().await.remove(&call_id);
    });
}

fn parse_port_range(s: &str) -> Result<(u16, u16), String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 2 {
        return Err("RTP port range must be in start-end format, e.g. 16384-32767".to_string());
    }
    let start = parts[0]
        .parse::<u16>()
        .map_err(|e| format!("Invalid start port: {e}"))?;
    let end = parts[1]
        .parse::<u16>()
        .map_err(|e| format!("Invalid end port: {e}"))?;
    if start > end {
        return Err(format!("Start port ({start}) must be <= end port ({end})"));
    }
    Ok((start, end))
}

fn validate_config(config: &ServerConfig) -> Result<(), String> {
    // 1. Built-in telephony validation
    config
        .telephony
        .validate()
        .map_err(|e| format!("Invalid telephony config: {e}"))?;

    // 2. Validate NAT advertised media settings
    if let Some(addr) = &config.telephony.advertised_media_address {
        if addr.trim().is_empty() {
            return Err("advertised_media_address must not be empty when set".to_string());
        }
    }

    // 3. Validate future TLS/SRTP placeholders
    if (config.tls_cert_path.is_some() && config.tls_key_path.is_none())
        || (config.tls_cert_path.is_none() && config.tls_key_path.is_some())
    {
        return Err(
            "Both tls_cert_path and tls_key_path must be provided if one is set".to_string(),
        );
    }

    if config.enable_srtp.unwrap_or(false) && config.tls_cert_path.is_none() {
        return Err("SRTP is enabled but no TLS cert/key is provided".to_string());
    }

    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");

    tokio::select! {
        _ = sigint.recv() => {
            info!("Received SIGINT signal, initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM signal, initiating graceful shutdown...");
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    info!("Received shutdown signal, initiating graceful shutdown...");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize structured logging
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "RemoteMedia Telephony Server starting"
    );

    // Build tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("telephony-worker")
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        // Load config file if specified, otherwise start with defaults
        let mut config = if let Some(config_path) = args.config {
            info!("Loading configuration from {:?}", config_path);
            let content = std::fs::read_to_string(&config_path)
                .map_err(|e| format!("Failed to read config file: {e}"))?;
            toml::from_str::<ServerConfig>(&content)
                .map_err(|e| format!("Failed to parse TOML config: {e}"))?
        } else {
            ServerConfig::default()
        };

        // Override with CLI flags
        if let Some(m) = args.manifest {
            config.manifest_path = Some(m.to_string_lossy().to_string());
        }
        if let Some(addr) = args.sip_bind_address {
            config.telephony.sip_bind_address = addr;
        }
        if let Some(addr) = args.advertised_media_address {
            config.telephony.advertised_media_address = Some(addr);
        }
        if let Some(range) = args.rtp_port_range {
            let (start, end) = parse_port_range(&range)?;
            config.telephony.rtp_port_start = start;
            config.telephony.rtp_port_end = end;
        }
        if let Some(peers) = args.allowed_peers {
            config.telephony.allowed_peers = peers;
        }
        if !args.access_mode.is_empty() {
            config.telephony.access_mode = match args.access_mode.as_str() {
                "denylist" | "deny_list" => remotemedia_telephony::SipAccessMode::DenyList,
                _ => remotemedia_telephony::SipAccessMode::AllowList,
            };
        }
        if let Some(peers) = args.blocked_peers {
            config.telephony.blocked_peers = peers;
        }
        if args.rate_limit_max.is_some()
            || args.rate_limit_window.is_some()
            || args.rate_limit_ban_duration.is_some()
        {
            config.telephony.rate_limit.enabled = true;
            if let Some(max) = args.rate_limit_max {
                config.telephony.rate_limit.max_requests_per_window = max;
            }
            if let Some(window) = args.rate_limit_window {
                config.telephony.rate_limit.window_seconds = window as u64;
            }
            if let Some(ban) = args.rate_limit_ban_duration {
                config.telephony.rate_limit.ban_duration_seconds = ban as u64;
            }
        }
        if let Some(siprec) = args.enable_siprec {
            config.telephony.enable_siprec = siprec;
        }
        if let Some(max_calls) = args.max_active_calls {
            config.telephony.max_active_calls = max_calls;
            if config.telephony.max_rtp_sessions < max_calls {
                config.telephony.max_rtp_sessions = max_calls;
            }
        }
        if let Some(cert) = args.tls_cert_path {
            config.tls_cert_path = Some(cert);
        }
        if let Some(key) = args.tls_key_path {
            config.tls_key_path = Some(key);
        }
        if let Some(srtp) = args.enable_srtp {
            config.enable_srtp = Some(srtp);
        }

        // Validate final merged configuration
        validate_config(&config)?;

        // Load RemoteMedia pipeline manifest JSON
        let manifest_path_str = config
            .manifest_path
            .as_deref()
            .ok_or("Pipeline manifest path is required (--manifest or manifest_path in config)")?;
        info!("Loading RemoteMedia pipeline manifest from {}", manifest_path_str);
        let manifest_content = std::fs::read_to_string(manifest_path_str)
            .map_err(|e| format!("Failed to read manifest file: {e}"))?;
        let manifest = Arc::new(manifest::parse(&manifest_content)?);
        manifest::validate(&manifest)?;

        // Initialize PipelineExecutor and TelephonyTransport
        let executor = Arc::new(PipelineExecutor::new().map_err(|e| format!("Failed to construct PipelineExecutor: {e}"))?);
        let transport = Arc::new(TelephonyTransport::new(config.telephony.clone(), executor).map_err(|e| format!("Failed to construct TelephonyTransport: {e}"))?);

        // Bind SIP UDP socket
        let socket = transport.bind_sip_socket().await.map_err(|e| format!("Failed to bind SIP socket: {e}"))?;
        let local_addr = socket.local_addr().expect("failed to obtain socket local address");
        info!("SIP signaling listener bound to UDP address {}", local_addr);

        // Warn if allowlist is empty (deny all)
        if matches!(config.telephony.access_mode, remotemedia_telephony::SipAccessMode::AllowList)
            && config.telephony.allowed_peers.is_empty()
        {
            warn!("SIP access control is in AllowList mode with empty allow-list — all peers will be rejected. Use --allowed-peers or set allowed_peers in config to allow specific peers.");
        }

        let socket = Arc::new(socket);
        let active_sessions: ActiveCalls = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Spawn signal handling task
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            wait_for_shutdown().await;
            let _ = shutdown_tx_clone.send(()).await;
        });

        // Run the main server loop
        let mut buf = vec![0_u8; config.telephony.max_sip_datagram_bytes];
        info!("Ready to accept telephony calls.");

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Initiating graceful server shutdown...");
                    break;
                }
                recv_res = socket.recv_from(&mut buf) => {
                    match recv_res {
                        Ok((len, peer)) => {
                            let datagram = &buf[..len];
                            match parse_request(datagram) {
                                Ok(request) => {
                                    // NOTE: Peer access control is handled by TelephonyTransport
                                    // (ensure_peer_allowed) — no duplicate check needed here.

                                    match request.method {
                                        SipMethod::Invite => {
                                            let call_id = request.call_id().unwrap_or("unknown").to_string();
                                            info!("Inbound call received (INVITE) from {}, Call-ID: {}", peer, call_id);

                                            let transport_clone = Arc::clone(&transport);
                                            let socket_clone = Arc::clone(&socket);
                                            let manifest_clone = Arc::clone(&manifest);
                                            let active_sessions_clone = Arc::clone(&active_sessions);
                                            let invite_bytes = datagram.to_vec();

                                            tokio::spawn(async move {
                                                match transport_clone.accept_inbound_call(&invite_bytes, manifest_clone).await {
                                                    Ok(accepted) => {
                                                        info!("Call negotiation successful for Call-ID: {}. Attaching pipeline session.", call_id);
                                                        if let Err(e) = socket_clone.send_to(&accepted.response.bytes, peer).await {
                                                            error!("Failed to send 200 OK SIP response to {}: {}", peer, e);
                                                        } else {
                                                            info!(
                                                                "RTP media: local {} -> remote {}, codec {:?}",
                                                                accepted.local_rtp_addr, accepted.remote_rtp_addr, accepted.negotiated_audio.codec
                                                            );
                                                            // The media loop owns the call's pipeline stream and RTP
                                                            // socket and closes them when it ends.
                                                            spawn_media_loop(
                                                                call_id.clone(),
                                                                CallMedia::from_accepted(accepted),
                                                                active_sessions_clone.clone(),
                                                            ).await;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Pipeline session creation failed for inbound call from {}: {}", peer, e);
                                                        // Format a SIP 500 error response
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
                                            info!("Terminating call (method: {:?}) for Call-ID: {}", request.method, call_id);

                                            // Teardown: stop the call's media loop, which closes its pipeline stream
                                            if let Some(stop) = active_sessions.lock().await.remove(&call_id) {
                                                info!("Tearing down pipeline session for call {}", call_id);
                                                let _ = stop.send(());
                                            }

                                            // Handle signaling Response
                                            match transport.handle_sip_datagram_from(datagram, peer) {
                                                Ok(Some(response)) => {
                                                    let _ = socket.send_to(&response.bytes, peer).await;
                                                }
                                                _ => {}
                                            }
                                        }
                                        SipMethod::Options => {
                                            match transport.handle_sip_datagram_from(datagram, peer) {
                                                Ok(Some(response)) => {
                                                    let _ = socket.send_to(&response.bytes, peer).await;
                                                }
                                                _ => {}
                                            }
                                        }
                                        SipMethod::Ack => {
                                            let _ = transport.handle_sip_datagram_from(datagram, peer);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Malformed SIP signaling request received from {}: {}", peer, e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error receiving SIP UDP packet: {}", e);
                        }
                    }
                }
            }
        }

        // Shutdown handling
        info!("Gracefully terminating active call sessions...");
        if let Err(e) = transport.shutdown().await {
            error!("Error shutting down telephony transport: {}", e);
        }

        // Close all active pipeline streams
        let mut active = active_sessions.lock().await;
        for (call_id, stop) in active.drain() {
            info!("Teardown: Closing pipeline session for call {} during server shutdown", call_id);
            let _ = stop.send(());
        }
        info!("Shutdown complete. Exiting.");
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::manifest::{Manifest, ManifestMetadata};
    use remotemedia_telephony::rtp::RtpPacket;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio::time::Duration;

    fn passthrough_manifest() -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "telephony-test".into(),
                ..Default::default()
            },
            nodes: vec![],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    #[test]
    fn test_parse_port_range() {
        assert_eq!(parse_port_range("16384-32767").unwrap(), (16384, 32767));
        assert!(parse_port_range("invalid").is_err());
        assert!(parse_port_range("100-50").is_err());
    }

    #[test]
    fn test_validate_config() {
        let mut config = ServerConfig::default();
        config.telephony.sip_bind_address = "127.0.0.1:5060".to_string();
        assert!(validate_config(&config).is_ok());

        // Validate TLS placeholders
        config.tls_cert_path = Some("/path/to/cert".to_string());
        assert!(validate_config(&config).is_err()); // Key is missing

        config.tls_key_path = Some("/path/to/key".to_string());
        assert!(validate_config(&config).is_ok()); // Both set

        config.enable_srtp = Some(true);
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_manifest_loading() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let manifest_path = std::path::Path::new(manifest_dir)
            .join("tests/fixtures/telephony_passthrough.json");
        let content = std::fs::read_to_string(manifest_path).expect("failed to read test manifest");
        let parsed = manifest::parse(&content).unwrap();
        assert_eq!(parsed.version, "v1");
        assert_eq!(parsed.metadata.name, "telephony-passthrough");
    }

    #[tokio::test]
    async fn test_udp_options_smoke() {
        let mut config = ServerConfig::default();
        config.telephony.sip_bind_address = "127.0.0.1:0".to_string();
        config.telephony.allowed_peers = vec!["127.0.0.1".into()];

        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let transport =
            Arc::new(TelephonyTransport::new(config.telephony.clone(), executor).unwrap());
        let socket = Arc::new(transport.bind_sip_socket().await.unwrap());
        let server_addr = socket.local_addr().unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);

        let socket_clone = Arc::clone(&socket);
        let transport_clone = Arc::clone(&transport);

        // Spawn signaling loop
        let handle = tokio::spawn(async move {
            let mut buf = vec![0_u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    res = socket_clone.recv_from(&mut buf) => {
                        if let Ok((len, peer)) = res {
                            let datagram = &buf[..len];
                            if let Ok(request) = parse_request(datagram) {
                                match request.method {
                                    SipMethod::Options => {
                                        if let Ok(Some(resp)) = transport_clone.handle_sip_datagram_from(datagram, peer) {
                                            let _ = socket_clone.send_to(&resp.bytes, peer).await;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        });

        // Send OPTIONS
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let options_req = b"OPTIONS sip:bot@example.com SIP/2.0\r\n\
            Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
            From: <sip:a@example.com>;tag=1\r\n\
            To: <sip:bot@example.com>\r\n\
            Call-ID: opt-test-1\r\n\
            CSeq: 1 OPTIONS\r\n\
            Content-Length: 0\r\n\r\n";
        client.send_to(options_req, server_addr).await.unwrap();

        let mut resp_buf = [0_u8; 1024];
        let (resp_len, _) =
            tokio::time::timeout(Duration::from_millis(1000), client.recv_from(&mut resp_buf))
                .await
                .unwrap()
                .unwrap();

        let resp_str = std::str::from_utf8(&resp_buf[..resp_len]).unwrap();
        assert!(resp_str.starts_with("SIP/2.0 200 OK"));

        // Shutdown
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_udp_invite_bye_smoke() {
        let mut config = ServerConfig::default();
        config.telephony.sip_bind_address = "127.0.0.1:0".to_string();
        config.telephony.allowed_peers = vec!["127.0.0.1".into()];
        config.telephony.advertised_media_address = Some("127.0.0.1".to_string());
        config.telephony.rtp_port_start = 25000;
        config.telephony.rtp_port_end = 25010;

        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let transport =
            Arc::new(TelephonyTransport::new(config.telephony.clone(), executor).unwrap());
        let socket = Arc::new(transport.bind_sip_socket().await.unwrap());
        let server_addr = socket.local_addr().unwrap();

        let active_sessions: ActiveCalls = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);

        let socket_clone = Arc::clone(&socket);
        let transport_clone = Arc::clone(&transport);
        let active_sessions_clone = Arc::clone(&active_sessions);
        let manifest_clone = passthrough_manifest();

        // Spawn signaling loop
        let handle = tokio::spawn(async move {
            let mut buf = vec![0_u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    res = socket_clone.recv_from(&mut buf) => {
                        if let Ok((len, peer)) = res {
                            let datagram = &buf[..len];
                            if let Ok(request) = parse_request(datagram) {
                                match request.method {
                                    SipMethod::Invite => {
                                        let call_id = request.call_id().unwrap_or("unknown").to_string();
                                        if let Ok(accepted) = transport_clone.accept_inbound_call(datagram, manifest_clone.clone()).await {
                                            let _ = socket_clone.send_to(&accepted.response.bytes, peer).await;
                                            spawn_media_loop(call_id, CallMedia::from_accepted(accepted), active_sessions_clone.clone()).await;
                                        }
                                    }
                                    SipMethod::Bye => {
                                        let call_id = request.call_id().unwrap_or("unknown").to_string();
                                        if let Some(stop) = active_sessions_clone.lock().await.remove(&call_id) {
                                            let _ = stop.send(());
                                        }
                                        if let Ok(Some(resp)) = transport_clone.handle_sip_datagram_from(datagram, peer) {
                                            let _ = socket_clone.send_to(&resp.bytes, peer).await;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        });

        // Send INVITE
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // The caller's RTP socket, offered in the SDP.
        let caller_rtp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let invite_req = format!(
            "INVITE sip:bot@example.com SIP/2.0\r\n\
            Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
            From: <sip:a@example.com>;tag=1\r\n\
            To: <sip:bot@example.com>\r\n\
            Call-ID: call-smoke-1\r\n\
            CSeq: 1 INVITE\r\n\
            Content-Type: application/sdp\r\n\
            Content-Length: 125\r\n\r\n\
            v=0\r\n\
            o=a 1 1 IN IP4 127.0.0.1\r\n\
            s=-\r\n\
            c=IN IP4 127.0.0.1\r\n\
            t=0 0\r\n\
            m=audio {} RTP/AVP 0\r\n\
            a=rtpmap:0 PCMU/8000\r\n\
            a=ptime:20\r\n",
            caller_rtp.local_addr().unwrap().port()
        );
        client.send_to(invite_req.as_bytes(), server_addr).await.unwrap();

        let mut resp_buf = [0_u8; 2048];
        let (resp_len, _) =
            tokio::time::timeout(Duration::from_millis(1500), client.recv_from(&mut resp_buf))
                .await
                .unwrap()
                .unwrap();

        let resp_str = std::str::from_utf8(&resp_buf[..resp_len]).unwrap();
        assert!(resp_str.starts_with("SIP/2.0 200 OK"));

        // The call's media loop runs: its RTP reaches the caller.
        let mut rtp_buf = [0_u8; 1500];
        let (rtp_len, _) =
            tokio::time::timeout(Duration::from_millis(1500), caller_rtp.recv_from(&mut rtp_buf))
                .await
                .expect("RTP from the call's media loop")
                .unwrap();
        let packet = RtpPacket::parse(&rtp_buf[..rtp_len]).unwrap();
        assert_eq!((packet.info.payload_type, packet.payload.len()), (0, 160));

        // Verify session is active
        {
            let active = active_sessions.lock().await;
            assert!(active.contains_key("call-smoke-1"));
        }

        // Send BYE
        let bye_req = b"BYE sip:bot@example.com SIP/2.0\r\n\
            Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK2\r\n\
            From: <sip:a@example.com>;tag=1\r\n\
            To: <sip:bot@example.com>;tag=remotemedia\r\n\
            Call-ID: call-smoke-1\r\n\
            CSeq: 2 BYE\r\n\
            Content-Length: 0\r\n\r\n";
        client.send_to(bye_req, server_addr).await.unwrap();

        let (resp_len, _) =
            tokio::time::timeout(Duration::from_millis(1000), client.recv_from(&mut resp_buf))
                .await
                .unwrap()
                .unwrap();

        let resp_str = std::str::from_utf8(&resp_buf[..resp_len]).unwrap();
        assert!(resp_str.starts_with("SIP/2.0 200 OK"));

        // Verify session is removed
        {
            let active = active_sessions.lock().await;
            assert!(!active.contains_key("call-smoke-1"));
        }

        // Shutdown
        let _ = shutdown_tx.send(()).await;
        let _ = handle.await;
    }
}
