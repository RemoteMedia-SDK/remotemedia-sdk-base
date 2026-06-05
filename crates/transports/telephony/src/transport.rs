//! Telephony transport implementation.

use crate::codec::CodecMap;
use crate::rtp::{RtpPortAllocator, RtpPortLease};
use crate::sdp::NegotiatedAudio;
use crate::sdp::{build_audio_answer, negotiate_audio, parse_audio_offer};
use crate::session::{
    metadata_keys, CallDirection, CallSession, CallSessionRegistry, CallSessionState,
    ParticipantRole,
};
use crate::sip::{
    build_method_not_allowed, build_options_ok, build_response, extract_raw_method, parse_request,
    SipMethod, SipRequest, SipTransactionResponse, SUPPORTED_METHODS,
};
use crate::CallMetrics;
use crate::{Error, Result, SipAccessMode, TelephonyTransportConfig};
use async_trait::async_trait;
use dashmap::DashMap;
use ipnet::IpNet;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    data::participant, Participant, ParticipantSessionHandle, PipelineExecutor, PipelineTransport,
    SharedPipelineOutputReceivers, StreamSession, TransportData,
};
use remotemedia_core::Result as CoreResult;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::UdpSocket;
use tracing::warn;

/// SIP/RTP telephony transport.
///
/// The transport owns protocol and socket lifecycle, then delegates pipeline
/// execution to `PipelineExecutor` once a call is represented as streaming
/// media frames.
pub struct TelephonyTransport {
    config: TelephonyTransportConfig,
    executor: Arc<PipelineExecutor>,
    sessions: CallSessionRegistry,
    rtp_ports: RtpPortAllocator,
    metrics: Arc<Mutex<CallMetrics>>,
    rate_limits: DashMap<IpAddr, PeerRateState>,
}

/// Per-peer rate-limit state.
struct PeerRateState {
    /// Timestamps of requests within the current sliding window.
    timestamps: Vec<Instant>,
    /// When the peer was banned until.
    banned_until: Option<Instant>,
}

impl Default for PeerRateState {
    fn default() -> Self {
        Self {
            timestamps: Vec::new(),
            banned_until: None,
        }
    }
}

/// Result of peer access control check.
enum PeerAccess {
    Allowed,
    Rejected,
}

struct TelephonyParticipantStream {
    participant: ParticipantSessionHandle,
    outputs: SharedPipelineOutputReceivers,
    closed: bool,
}

#[async_trait]
impl StreamSession for TelephonyParticipantStream {
    fn session_id(&self) -> &str {
        self.participant.session_id()
    }

    async fn send_input(&mut self, data: TransportData) -> CoreResult<()> {
        if self.closed {
            return Err(remotemedia_core::Error::Execution(format!(
                "telephony participant stream for session {} is closed",
                self.session_id()
            )));
        }
        self.participant.send(data).await
    }

    async fn recv_output(&mut self) -> CoreResult<Option<TransportData>> {
        self.outputs.recv_output().await
    }

    async fn close(&mut self) -> CoreResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        Ok(())
    }

    fn is_active(&self) -> bool {
        !self.closed
    }
}

/// Accepted inbound SIP call attached to a pipeline stream session.
pub struct AcceptedCall {
    /// SIP response to send to the caller.
    pub response: SipTransactionResponse,
    /// RemoteMedia streaming session for decoded inbound/outbound audio.
    pub stream: Box<dyn StreamSession>,
    /// Bound RTP UDP socket for the accepted media leg.
    pub rtp_socket: UdpSocket,
    /// Local RTP address bound for this call.
    pub local_rtp_addr: SocketAddr,
    /// Remote RTP address from the caller SDP offer.
    pub remote_rtp_addr: SocketAddr,
    /// Negotiated codec and packetization parameters.
    pub negotiated_audio: NegotiatedAudio,
    /// RTP payload mappings accepted for inbound media.
    pub codec_map: CodecMap,
    /// SIP Call-ID associated with the media leg.
    pub call_id: String,
    /// RemoteMedia call leg identifier for caller media.
    pub leg_id: String,
    _rtp_port: RtpPortLease,
}

struct PreparedInboundCall {
    response: SipTransactionResponse,
    rtp_port: RtpPortLease,
    local_rtp_addr: SocketAddr,
    remote_rtp_addr: SocketAddr,
    negotiated_audio: NegotiatedAudio,
    codec_map: CodecMap,
    call_id: String,
    leg_id: String,
}

enum InboundInvitePreparation {
    Accepted(PreparedInboundCall),
    Rejected(SipTransactionResponse),
}

impl TelephonyTransport {
    /// Create a telephony transport with validated configuration.
    pub fn new(config: TelephonyTransportConfig, executor: Arc<PipelineExecutor>) -> Result<Self> {
        config.validate()?;
        let rtp_ports = RtpPortAllocator::new(config.rtp_port_start, config.rtp_port_end)?;
        Ok(Self {
            config,
            executor,
            sessions: CallSessionRegistry::default(),
            rtp_ports,
            metrics: Arc::new(Mutex::new(CallMetrics::default())),
            rate_limits: DashMap::new(),
        })
    }

    /// Return transport configuration.
    pub fn config(&self) -> &TelephonyTransportConfig {
        &self.config
    }

    /// Return the in-memory call session registry.
    pub fn sessions(&self) -> &CallSessionRegistry {
        &self.sessions
    }

    fn shared_session_key(&self) -> String {
        self.config
            .shared_session_key
            .as_deref()
            .filter(|key| !key.is_empty())
            .unwrap_or("default")
            .to_string()
    }

    /// Return a metrics snapshot.
    pub fn metrics_snapshot(&self) -> Result<CallMetrics> {
        self.metrics
            .lock()
            .map(|metrics| metrics.clone())
            .map_err(|_| Error::Session("telephony metrics lock poisoned".to_string()))
    }

    /// Bind the configured SIP UDP socket.
    pub async fn bind_sip_socket(&self) -> Result<UdpSocket> {
        UdpSocket::bind(&self.config.sip_bind_address)
            .await
            .map_err(Error::Io)
    }

    /// Receive and handle a single SIP UDP datagram.
    pub async fn recv_sip_once(&self, socket: &UdpSocket) -> Result<Option<SocketAddr>> {
        // Periodically evict expired rate limit entries
        self.evict_expired_rate_limits();

        let mut buf = vec![0_u8; self.config.max_sip_datagram_bytes];
        let (len, peer) = socket.recv_from(&mut buf).await?;
        if let Some(response) = self.handle_sip_datagram_from(&buf[..len], peer)? {
            socket.send_to(&response.bytes, peer).await?;
        }
        Ok(Some(peer))
    }

    /// Handle one SIP datagram and produce a SIP response when appropriate.
    pub fn handle_sip_datagram(&self, datagram: &[u8]) -> Result<Option<SipTransactionResponse>> {
        self.handle_sip_datagram_inner(datagram, None)
    }

    /// Handle one SIP datagram from a known peer.
    pub fn handle_sip_datagram_from(
        &self,
        datagram: &[u8],
        peer: SocketAddr,
    ) -> Result<Option<SipTransactionResponse>> {
        self.handle_sip_datagram_inner(datagram, Some(peer))
    }

    fn handle_sip_datagram_inner(
        &self,
        datagram: &[u8],
        peer: Option<SocketAddr>,
    ) -> Result<Option<SipTransactionResponse>> {
        if datagram.len() > self.config.max_sip_datagram_bytes {
            return Err(Error::Sip(format!(
                "SIP datagram too large: {} bytes",
                datagram.len()
            )));
        }

        // Phase 1: Access control (before any parsing)
        if let Some(peer) = peer {
            match self.ensure_peer_allowed_result(peer) {
                PeerAccess::Allowed => {}
                PeerAccess::Rejected => {
                    // Build minimal 403 response from raw bytes
                    let via = crate::sip::extract_via_from_raw(datagram);
                    let mut resp = String::from("SIP/2.0 403 Forbidden\r\n");
                    if let Some(v) = via {
                        resp.push_str(&format!("Via: {v}\r\n"));
                    }
                    resp.push_str("Server: RemoteMedia Telephony\r\n");
                    resp.push_str("Content-Length: 0\r\n\r\n");
                    return Ok(Some(SipTransactionResponse {
                        status_code: 403,
                        bytes: resp.into_bytes(),
                    }));
                }
            }
        }

        // Phase 2: Pre-parse method detection (405 for unsupported methods)
        // This avoids wasting CPU on malformed requests from scanners
        if let Some(method) = extract_raw_method(datagram) {
            if !SipMethod::parse(method).is_some() {
                // Unsupported method — send 405 without full parsing
                return Ok(Some(build_method_not_allowed(datagram)));
            }
        }

        // Phase 3: Rate limiting (after access control, before parsing)
        if let Some(peer) = peer {
            if let Some(rate_response) = self.check_rate_limit_response(peer.ip()) {
                return Ok(Some(rate_response));
            }
        }

        // Phase 4: Full SIP parsing
        let request = parse_request(datagram)?;
        match request.method {
            SipMethod::Options => Ok(Some(SipTransactionResponse {
                status_code: 200,
                bytes: build_options_ok(&request).into_bytes(),
            })),
            SipMethod::Invite => self.handle_invite(&request).map(Some),
            SipMethod::Bye | SipMethod::Cancel => {
                if let Some(call_id) = request.call_id() {
                    if self.sessions.get(call_id)?.is_some() {
                        self.sessions
                            .transition(call_id, CallSessionState::Terminating)?;
                        self.sessions
                            .transition(call_id, CallSessionState::Terminated)?;
                        if let Ok(mut metrics) = self.metrics.lock() {
                            metrics.record_call_completed();
                        }
                    }
                }
                Ok(Some(SipTransactionResponse {
                    status_code: 200,
                    bytes: build_response(&request, 200, "OK", "", None).into_bytes(),
                }))
            }
            SipMethod::Ack => Ok(None),
        }
    }

    fn ensure_peer_allowed_result(&self, peer: SocketAddr) -> PeerAccess {
        match self.config.access_mode {
            SipAccessMode::AllowList => {
                if self.config.allowed_peers.is_empty() {
                    return PeerAccess::Rejected;
                }
                if self
                    .config
                    .allowed_peers
                    .iter()
                    .any(|entry| peer_allowed_by_entry(entry, peer))
                {
                    PeerAccess::Allowed
                } else {
                    PeerAccess::Rejected
                }
            }
            SipAccessMode::DenyList => {
                if self
                    .config
                    .blocked_peers
                    .iter()
                    .any(|entry| peer_allowed_by_entry(entry, peer))
                {
                    PeerAccess::Rejected
                } else {
                    PeerAccess::Allowed
                }
            }
        }
    }

    fn check_rate_limit_response(&self, peer_ip: IpAddr) -> Option<SipTransactionResponse> {
        if !self.config.rate_limit.enabled {
            return None;
        }

        let now = Instant::now();
        let config = &self.config.rate_limit;

        let mut entry = self
            .rate_limits
            .entry(peer_ip)
            .or_insert_with(|| PeerRateState::default());

        // Check if banned
        if let Some(banned_until) = entry.banned_until {
            if now < banned_until {
                let mut resp = String::from("SIP/2.0 503 Service Unavailable\r\n");
                resp.push_str("Server: RemoteMedia Telephony\r\n");
                resp.push_str("Content-Length: 0\r\n\r\n");
                return Some(SipTransactionResponse {
                    status_code: 503,
                    bytes: resp.into_bytes(),
                });
            }
            // Ban expired, reset state
            entry.timestamps.clear();
            entry.banned_until = None;
        }

        // Remove expired timestamps
        let window_start = now - std::time::Duration::from_secs(config.window_seconds);
        entry.timestamps.retain(|&t| t > window_start);

        // Check rate
        if entry.timestamps.len() >= config.max_requests_per_window as usize {
            entry.banned_until =
                Some(now + std::time::Duration::from_secs(config.ban_duration_seconds));
            let mut resp = String::from("SIP/2.0 503 Service Unavailable\r\n");
            resp.push_str("Server: RemoteMedia Telephony\r\n");
            resp.push_str("Content-Length: 0\r\n\r\n");
            return Some(SipTransactionResponse {
                status_code: 503,
                bytes: resp.into_bytes(),
            });
        }

        entry.timestamps.push(now);
        None
    }

    fn evict_expired_rate_limits(&self) {
        if !self.config.rate_limit.enabled {
            return;
        }

        let now = std::time::Instant::now();
        let ban_duration =
            std::time::Duration::from_secs(self.config.rate_limit.ban_duration_seconds);
        let window_duration = std::time::Duration::from_secs(self.config.rate_limit.window_seconds);

        self.rate_limits.retain(|_, entry| {
            // Keep if recently banned
            if let Some(banned_until) = entry.banned_until {
                now < banned_until
            } else {
                // Keep if has recent timestamps
                entry.timestamps.iter().any(|&t| now - t < window_duration)
            }
        });
    }

    /// Check if a peer socket address is allowed by a single peer entry.
    ///
    /// Supports: exact IP, full socket address, CIDR notation, wildcard `*`.
    fn peer_allowed_by_entry(&self, entry: &str, peer: SocketAddr) -> bool {
        peer_allowed_by_entry(entry, peer)
    }

    fn handle_invite(&self, request: &SipRequest) -> Result<SipTransactionResponse> {
        match self.prepare_inbound_call(request)? {
            InboundInvitePreparation::Accepted(prepared) => Ok(prepared.response),
            InboundInvitePreparation::Rejected(response) => Ok(response),
        }
    }

    fn prepare_inbound_call(&self, request: &SipRequest) -> Result<InboundInvitePreparation> {
        let call_id = request
            .call_id()
            .ok_or_else(|| Error::Sip("INVITE missing Call-ID".to_string()))?
            .to_string();

        if self.sessions.get(&call_id)?.is_none()
            && self.sessions.len()? >= self.config.max_active_calls as usize
        {
            return Ok(InboundInvitePreparation::Rejected(SipTransactionResponse {
                status_code: 486,
                bytes: build_response(request, 486, "Busy Here", "", None).into_bytes(),
            }));
        }

        let offer = match parse_audio_offer(&request.body) {
            Ok(offer) => offer,
            Err(e) => {
                let body_preview: String = request.body.chars().take(512).collect();
                warn!(
                    error = %e,
                    body_len = request.body.len(),
                    body_preview = %body_preview,
                    "failed to parse SIP INVITE SDP offer"
                );
                return Ok(InboundInvitePreparation::Rejected(SipTransactionResponse {
                    status_code: 400,
                    bytes: build_response(request, 400, "Bad Request", "", None).into_bytes(),
                }));
            }
        };

        let negotiated = match negotiate_audio(&offer, &self.config) {
            Ok(negotiated) => negotiated,
            Err(_) => {
                return Ok(InboundInvitePreparation::Rejected(SipTransactionResponse {
                    status_code: 488,
                    bytes: build_response(request, 488, "Not Acceptable Here", "", None)
                        .into_bytes(),
                }));
            }
        };

        let rtp_port = self.rtp_ports.allocate()?;
        let local_rtp_addr = rtp_port.socket_addr();
        let remote_ip = offer.connection_address.parse::<IpAddr>().map_err(|e| {
            Error::Sdp(format!(
                "invalid remote SDP connection address '{}': {e}",
                offer.connection_address
            ))
        })?;
        let remote_rtp_addr = SocketAddr::new(remote_ip, offer.port);
        let media_address = self
            .config
            .advertised_media_address
            .as_deref()
            .unwrap_or("127.0.0.1");
        let answer = build_audio_answer(media_address, rtp_port.port(), &negotiated);
        let mut codec_map = CodecMap::default();
        codec_map.insert(negotiated.payload_type, negotiated.codec)?;
        for payload_type in &offer.payload_types {
            if let Some(codec) = offer.rtpmap.get(payload_type) {
                let _ = codec_map.insert(*payload_type, *codec);
            }
        }

        let sip_port = self
            .config
            .sip_bind_address
            .split(':')
            .last()
            .unwrap_or("5060");
        let contact_host = if let Some(media_ip) = &self.config.advertised_media_address {
            format!("{media_ip}:{sip_port}")
        } else if self.config.sip_bind_address.starts_with("0.0.0.0:") {
            request
                .uri
                .split('@')
                .nth(1)
                .unwrap_or("127.0.0.1")
                .to_string()
        } else {
            self.config.sip_bind_address.clone()
        };

        let mut session = CallSession::new(call_id.clone(), CallDirection::Inbound);
        session.external_call_id = Some(call_id.clone());
        let leg_id = format!("{call_id}:caller");
        session.legs.push(leg_id.clone());
        session.transition_to(CallSessionState::Active)?;
        if self.sessions.get(&call_id)?.is_none() {
            self.sessions.insert(session)?;
            if let Ok(mut metrics) = self.metrics.lock() {
                metrics.record_call_started();
            }
        }

        Ok(InboundInvitePreparation::Accepted(PreparedInboundCall {
            response: SipTransactionResponse {
                status_code: 200,
                bytes: build_response(request, 200, "OK", &answer, Some(&contact_host))
                    .into_bytes(),
            },
            rtp_port,
            local_rtp_addr,
            remote_rtp_addr,
            negotiated_audio: negotiated,
            codec_map,
            call_id,
            leg_id,
        }))
    }

    /// Accept an inbound INVITE and attach it to a RemoteMedia streaming session.
    pub async fn accept_inbound_call(
        &self,
        invite_datagram: &[u8],
        manifest: Arc<Manifest>,
    ) -> Result<AcceptedCall> {
        let request = parse_request(invite_datagram)?;
        if request.method != SipMethod::Invite {
            return Err(Error::Sip("expected SIP INVITE".to_string()));
        }
        let prepared = match self.prepare_inbound_call(&request)? {
            InboundInvitePreparation::Accepted(prepared) => prepared,
            InboundInvitePreparation::Rejected(response) => {
                return Err(Error::Sip(format!(
                    "INVITE rejected with {}",
                    response.status_code
                )))
            }
        };
        let rtp_socket = UdpSocket::bind(prepared.local_rtp_addr)
            .await
            .map_err(Error::Io)?;
        let stream = self
            .participant_stream(
                manifest,
                &prepared.call_id,
                &prepared.leg_id,
                ParticipantRole::User,
            )
            .await
            .map_err(|e| Error::Pipeline(e.to_string()))?;
        Ok(AcceptedCall {
            response: prepared.response,
            stream,
            rtp_socket,
            local_rtp_addr: prepared.local_rtp_addr,
            remote_rtp_addr: prepared.remote_rtp_addr,
            negotiated_audio: prepared.negotiated_audio,
            codec_map: prepared.codec_map,
            call_id: prepared.call_id,
            leg_id: prepared.leg_id,
            _rtp_port: prepared.rtp_port,
        })
    }

    async fn participant_stream(
        &self,
        manifest: Arc<Manifest>,
        call_id: &str,
        leg_id: &str,
        role: ParticipantRole,
    ) -> remotemedia_core::Result<Box<dyn StreamSession>> {
        let shared = self
            .executor
            .get_or_create_shared_session(self.shared_session_key(), manifest)
            .await?;
        let participant = Participant::new(leg_id.to_string(), role.as_participant_role())
            .with_track_id(leg_id.to_string())
            .with_modality(participant::modality::AUDIO)
            .with_metadata(metadata_keys::CALL_ID, call_id)
            .with_metadata(metadata_keys::LEG_ID, leg_id)
            .with_metadata(
                metadata_keys::PARTICIPANT_ROLE,
                format!("{role:?}").to_lowercase(),
            );

        Ok(Box::new(TelephonyParticipantStream {
            participant: shared.participant_handle(participant),
            outputs: shared.subscribe_outputs(),
            closed: false,
        }))
    }

    /// Gracefully terminate all tracked call sessions.
    pub async fn shutdown(&self) -> Result<()> {
        for call_id in self.sessions.call_ids()? {
            if let Some(session) = self.sessions.get(&call_id)? {
                if !matches!(
                    session.state,
                    CallSessionState::Terminating | CallSessionState::Terminated
                ) {
                    self.sessions
                        .transition(&call_id, CallSessionState::Terminating)?;
                }
                if self.sessions.get(&call_id)?.is_some() {
                    self.sessions
                        .transition(&call_id, CallSessionState::Terminated)?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PipelineTransport for TelephonyTransport {
    async fn execute(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> remotemedia_core::Result<TransportData> {
        self.executor.execute_unary(manifest, input).await
    }

    async fn stream(
        &self,
        manifest: Arc<Manifest>,
    ) -> remotemedia_core::Result<Box<dyn StreamSession>> {
        let session = self.executor.create_session(manifest).await?;
        Ok(Box::new(session))
    }
}

/// Check if a peer socket address is allowed by a single peer entry.
///
/// Supports: exact IP, full socket address, CIDR notation, wildcard `*`.
fn peer_allowed_by_entry(entry: &str, peer: SocketAddr) -> bool {
    let entry = entry.trim();

    // Wildcard allows all
    if entry == "*" {
        return true;
    }

    // Try CIDR notation first
    if entry.contains('/') {
        if let Ok(cidr) = IpNet::from_str(entry) {
            return cidr.contains(&peer.ip());
        }
    }

    // Try full socket address (IP:port)
    if let Ok(addr) = entry.parse::<SocketAddr>() {
        return addr == peer;
    }

    // Try exact IP address
    if let Ok(ip) = entry.parse::<IpAddr>() {
        return peer.ip() == ip;
    }

    // Unknown format — reject
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::manifest::{Manifest, ManifestMetadata};
    use std::time::{Duration, Instant};

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
    fn new_rejects_invalid_config() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            codec_preferences: Vec::new(),
            ..TelephonyTransportConfig::default()
        };

        assert!(TelephonyTransport::new(config, executor).is_err());
    }

    #[test]
    fn new_accepts_default_config() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let transport = TelephonyTransport::new(TelephonyTransportConfig::default(), executor)
            .expect("default telephony config should be valid");

        assert_eq!(transport.config().frame_duration_ms, 20);
    }

    #[test]
    fn handles_options_request() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let transport = TelephonyTransport::new(TelephonyTransportConfig::default(), executor)
            .expect("default telephony config should be valid");
        let response = transport
            .handle_sip_datagram(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: opt-1\r\n\
CSeq: 1 OPTIONS\r\n\
Content-Length: 0\r\n\r\n",
            )
            .unwrap()
            .unwrap();
        assert_eq!(response.status_code, 200);
        assert!(String::from_utf8(response.bytes)
            .unwrap()
            .contains("Allow: INVITE"));
    }

    #[test]
    fn handles_invite_and_bye_lifecycle() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            sip_bind_address: "127.0.0.1:5060".into(),
            advertised_media_address: Some("127.0.0.1".into()),
            rtp_port_start: 20_000,
            rtp_port_end: 20_010,
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let invite = b"INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-1\r\n\
CSeq: 1 INVITE\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 0\r\n\r\n\
v=0\r\n\
o=a 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n";
        let response = transport.handle_sip_datagram(invite).unwrap().unwrap();
        assert_eq!(response.status_code, 200);
        assert_eq!(
            transport.sessions().get("call-1").unwrap().unwrap().state,
            CallSessionState::Active
        );

        let bye = b"BYE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK2\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>;tag=remotemedia\r\n\
Call-ID: call-1\r\n\
CSeq: 2 BYE\r\n\
Content-Length: 0\r\n\r\n";
        transport.handle_sip_datagram(bye).unwrap().unwrap();
        assert_eq!(
            transport.sessions().get("call-1").unwrap().unwrap().state,
            CallSessionState::Terminated
        );
        assert_eq!(transport.metrics_snapshot().unwrap().completed_calls, 1);
    }

    #[test]
    fn rejects_disallowed_peer() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["192.0.2.1".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "127.0.0.1:5060".parse().unwrap(),
            )
            .unwrap();
        // Disallowed peer returns 403 Forbidden
        let response = result.expect("expected 403 response");
        assert_eq!(response.status_code, 403);
    }

    #[test]
    fn allowlist_empty_peers_rejects_all() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            // allowed_peers is empty — deny all
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 403 response");
        assert_eq!(response.status_code, 403);
    }

    #[test]
    fn allowlist_cidr_match_allows_peer() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["10.0.0.0/8".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "10.1.2.3:5060".parse().unwrap(),
            )
            .unwrap();
        // CIDR match — peer allowed, OPTIONS returns 200
        let response = result.expect("expected 200 response");
        assert_eq!(response.status_code, 200);
    }

    #[test]
    fn allowlist_cidr_no_match_rejects_peer() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["10.0.0.0/8".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.168.1.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 403 response");
        assert_eq!(response.status_code, 403);
    }

    #[test]
    fn denylist_blocks_peer() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            access_mode: crate::SipAccessMode::DenyList,
            blocked_peers: vec!["10.0.0.5".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "10.0.0.5:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 403 response");
        assert_eq!(response.status_code, 403);
    }

    #[test]
    fn denylist_empty_blocks_none() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            access_mode: crate::SipAccessMode::DenyList,
            blocked_peers: vec![],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "10.0.0.5:5060".parse().unwrap(),
            )
            .unwrap();
        // Not blocked — OPTIONS returns 200
        let response = result.expect("expected 200 response");
        assert_eq!(response.status_code, 200);
    }

    #[test]
    fn register_returns_405_method_not_allowed() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"REGISTER sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 405 response");
        assert_eq!(response.status_code, 405);
        let body = std::str::from_utf8(&response.bytes).unwrap();
        assert!(body.contains("Allow:"));
    }

    #[test]
    fn subscribe_returns_405_method_not_allowed() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"SUBSCRIBE sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 405 response");
        assert_eq!(response.status_code, 405);
    }

    #[test]
    fn forbitden_response_structure() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["192.0.2.1".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "127.0.0.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 403 response");
        assert_eq!(response.status_code, 403);
        let body = std::str::from_utf8(&response.bytes).unwrap();
        assert!(body.starts_with("SIP/2.0 403 Forbidden"));
    }

    #[test]
    fn rate_limit_disabled_allows_all() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            rate_limit: crate::SipRateLimitConfig {
                enabled: false,
                ..Default::default()
            },
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        // Send 100 OPTIONS requests — should all succeed
        for _ in 0..100 {
            let result = transport
                .handle_sip_datagram_from(
                    b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                    "192.0.2.1:5060".parse().unwrap(),
                )
                .unwrap();
            let response = result.expect("expected 200 response");
            assert_eq!(response.status_code, 200);
        }
    }

    #[test]
    fn rate_limit_enforces_window() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            rate_limit: crate::SipRateLimitConfig {
                enabled: true,
                max_requests_per_window: 5,
                window_seconds: 60,
                ban_duration_seconds: 60,
            },
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        // First 5 requests succeed
        for _ in 0..5 {
            let result = transport
                .handle_sip_datagram_from(
                    b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                    "192.0.2.1:5060".parse().unwrap(),
                )
                .unwrap();
            let response = result.expect("expected 200 response");
            assert_eq!(response.status_code, 200);
        }
        // 6th request is rate-limited (503)
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 503 response");
        assert_eq!(response.status_code, 503);
    }

    #[test]
    fn rate_limit_different_peers_independent() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            rate_limit: crate::SipRateLimitConfig {
                enabled: true,
                max_requests_per_window: 3,
                window_seconds: 60,
                ban_duration_seconds: 60,
            },
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        // Peer A sends 3 requests
        for _ in 0..3 {
            let result = transport
                .handle_sip_datagram_from(
                    b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                    "192.0.2.1:5060".parse().unwrap(),
                )
                .unwrap();
            let response = result.expect("expected 200 response");
            assert_eq!(response.status_code, 200);
        }
        // Peer A is rate-limited
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 503 response");
        assert_eq!(response.status_code, 503);
        // Peer B is not rate-limited (independent)
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.2:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 200 response");
        assert_eq!(response.status_code, 200);
    }

    #[test]
    fn method_not_allowed_does_not_consume_rate_limit() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            allowed_peers: vec!["*".into()],
            rate_limit: crate::SipRateLimitConfig {
                enabled: true,
                max_requests_per_window: 3,
                window_seconds: 60,
                ban_duration_seconds: 60,
            },
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        // Send 100 REGISTER requests — should all get 405, not 503
        for _ in 0..100 {
            let result = transport
                .handle_sip_datagram_from(
                    b"REGISTER sip:bot@example.com SIP/2.0\r\n\r\n",
                    "192.0.2.1:5060".parse().unwrap(),
                )
                .unwrap();
            let response = result.expect("expected 405 response");
            assert_eq!(response.status_code, 405);
        }
        // After 100 REGISTERs, OPTIONS should still work (no rate limit consumed)
        let result = transport
            .handle_sip_datagram_from(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\r\n",
                "192.0.2.1:5060".parse().unwrap(),
            )
            .unwrap();
        let response = result.expect("expected 200 response");
        assert_eq!(response.status_code, 200);
    }

    #[tokio::test]
    async fn binds_and_handles_one_udp_sip_datagram() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            sip_bind_address: "127.0.0.1:0".into(),
            allowed_peers: vec!["127.0.0.1".into()],
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let socket = transport.bind_sip_socket().await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client
            .send_to(
                b"OPTIONS sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: opt-udp\r\n\
CSeq: 1 OPTIONS\r\n\
Content-Length: 0\r\n\r\n",
                server_addr,
            )
            .await
            .unwrap();

        transport.recv_sip_once(&socket).await.unwrap();

        let mut buf = [0_u8; 2048];
        let (len, _) = client.recv_from(&mut buf).await.unwrap();
        let response = std::str::from_utf8(&buf[..len]).unwrap();
        assert!(response.starts_with("SIP/2.0 200 OK"));
    }

    #[tokio::test]
    async fn accepts_inbound_call_with_streaming_session() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            sip_bind_address: "127.0.0.1:5060".into(),
            advertised_media_address: Some("127.0.0.1".into()),
            rtp_port_start: 20_000,
            rtp_port_end: 20_010,
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let accepted = transport
            .accept_inbound_call(
                b"INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-stream\r\n\
CSeq: 1 INVITE\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 0\r\n\r\n\
v=0\r\n\
o=a 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n",
                passthrough_manifest(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.response.status_code, 200);
        assert!(accepted.stream.is_active());
        assert_eq!(accepted.local_rtp_addr.port(), 20_000);
        assert_eq!(
            accepted.remote_rtp_addr,
            "127.0.0.1:49170".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(accepted.negotiated_audio.payload_type, 0);
        assert!(accepted.codec_map.get(0).is_some());
        assert_eq!(accepted.call_id, "call-stream");
        assert_eq!(accepted.leg_id, "call-stream:caller");
    }

    #[tokio::test]
    async fn accepted_calls_share_pipeline_session() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let config = TelephonyTransportConfig {
            sip_bind_address: "127.0.0.1:5060".into(),
            advertised_media_address: Some("127.0.0.1".into()),
            rtp_port_start: 20_100,
            rtp_port_end: 20_110,
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();
        let manifest = passthrough_manifest();

        let mut first = transport
            .accept_inbound_call(
                b"INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK1\r\n\
From: <sip:a@example.com>;tag=1\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-shared-1\r\n\
CSeq: 1 INVITE\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 0\r\n\r\n\
v=0\r\n\
o=a 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n",
                Arc::clone(&manifest),
            )
            .await
            .unwrap();
        let mut second = transport
            .accept_inbound_call(
                b"INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK2\r\n\
From: <sip:b@example.com>;tag=2\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-shared-2\r\n\
CSeq: 1 INVITE\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 0\r\n\r\n\
v=0\r\n\
o=b 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49172 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n",
                manifest,
            )
            .await
            .unwrap();

        assert_eq!(first.stream.session_id(), second.stream.session_id());
        assert_eq!(first.leg_id, "call-shared-1:caller");
        assert_eq!(second.leg_id, "call-shared-2:caller");

        first.stream.close().await.unwrap();
        assert!(second.stream.is_active());
        second.stream.close().await.unwrap();
    }

    #[tokio::test]
    async fn accepted_call_joins_existing_shared_pipeline_session() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let manifest = passthrough_manifest();
        let shared = executor
            .get_or_create_shared_session("room-a", Arc::clone(&manifest))
            .await
            .unwrap();
        let config = TelephonyTransportConfig {
            sip_bind_address: "127.0.0.1:5060".into(),
            advertised_media_address: Some("127.0.0.1".into()),
            rtp_port_start: 20_200,
            rtp_port_end: 20_210,
            shared_session_key: Some("room-a".into()),
            ..TelephonyTransportConfig::default()
        };
        let transport = TelephonyTransport::new(config, executor).unwrap();

        let mut accepted = transport
            .accept_inbound_call(
                b"INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK3\r\n\
From: <sip:c@example.com>;tag=3\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-existing-shared\r\n\
CSeq: 1 INVITE\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 0\r\n\r\n\
v=0\r\n\
o=c 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 49174 RTP/AVP 0\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=ptime:20\r\n",
                manifest,
            )
            .await
            .unwrap();

        assert_eq!(accepted.stream.session_id(), shared.session_id());
        accepted.stream.close().await.unwrap();
        assert!(shared.is_active().await);
        shared.close().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_terminates_active_calls() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let transport =
            TelephonyTransport::new(TelephonyTransportConfig::default(), executor).unwrap();
        transport
            .sessions()
            .insert(CallSession::new(
                "call-shutdown".into(),
                CallDirection::Inbound,
            ))
            .unwrap();
        transport
            .sessions()
            .transition("call-shutdown", CallSessionState::Active)
            .unwrap();
        transport.shutdown().await.unwrap();
        assert_eq!(
            transport
                .sessions()
                .get("call-shutdown")
                .unwrap()
                .unwrap()
                .state,
            CallSessionState::Terminated
        );
    }

    #[test]
    fn validates_rtp_ingress_latency_budget() {
        let mut codec_map = crate::codec::CodecMap::default();
        codec_map.insert(0, crate::AudioCodec::Pcmu).unwrap();
        let mut session = crate::rtp::RtpMediaSession::new(
            "call".into(),
            "caller".into(),
            crate::ParticipantRole::User,
            codec_map,
            3,
        );
        let packet = crate::rtp::RtpPacket {
            info: crate::rtp::RtpPacketInfo {
                payload_type: 0,
                sequence: 1,
                timestamp: 160,
                ssrc: 1,
                marker: false,
            },
            payload: vec![0xff; 160],
        };

        let start = Instant::now();
        let frames = session.receive_datagram(&packet.write()).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn validates_synthetic_tool_to_rtp_latency_budget() {
        let start = Instant::now();
        let tool_response = "synthetic database response";
        assert!(!tool_response.is_empty());
        let mut outbound =
            crate::rtp::RtpOutboundMediaSession::new(crate::AudioCodec::Pcmu, 0, 1, 1, 160, 20)
                .unwrap();
        let packet = outbound.packetize_audio_frame(&vec![0.0; 160]).unwrap();
        assert_eq!(packet.info.payload_type, 0);
        assert!(start.elapsed() < Duration::from_millis(400));
    }
}
