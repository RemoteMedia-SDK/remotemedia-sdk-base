//! The per-call RTP media loop, shared by every host of the telephony
//! transport (the `telephony-server` binary, the Python `TelephonyServer`).
//!
//! One task per answered call: inbound RTP is decoded into the pipeline
//! session, and the session's outputs are sent back as RTP, one packet per
//! frame duration. The send side is turn-aware (openspec:
//! add-ai-sdk-voice-library, D7 and D11):
//!
//! - audio is queued with the turn and text chunk in its metadata
//!   (`TurnTag::from_metadata`), and a tagged `speech_end` output is the
//!   chunk's end mark;
//! - what the call has released is published as `audio_released` on the
//!   session bus, for the call leg as the member;
//! - a targeted flush (`SessionControl::request_flush_audio_for`) drops one
//!   turn's queued audio for this call; the session-wide flush
//!   (`request_flush_audio`) drops everything queued.
//!
//! Packets are taken from the queue at the pace they are sent, so a report
//! is at most one frame ahead of the network.

#[cfg(test)]
#[path = "media_loop_tests.rs"]
mod tests;

use crate::rtp::{RtpMediaSession, RtpOutboundMediaSession, RtpPortLease};
use crate::session::ParticipantRole;
use crate::transport::AcceptedCall;
use crate::{AudioCodec, Result};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::data::participant;
use remotemedia_core::transport::session_control::SessionControl;
use remotemedia_core::transport::turn_audio::{FlushTarget, TurnTag};
use remotemedia_core::transport::{StreamSession, TransportData};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

/// Jitter buffer depth of a call's inbound RTP, in packets.
const JITTER_DEPTH_PACKETS: usize = 4;
/// Silence frames sent before media to open NAT bindings toward the caller.
const NAT_PUNCH_FRAMES: usize = 5;
const OUTBOUND_SSRC: u32 = 0xABCD_EF01;

/// The send side of one call: the session's outputs in, paced RTP datagrams
/// out, `audio_released` reports to the session bus.
pub struct CallAudioSender {
    outbound: RtpOutboundMediaSession,
    /// The call leg's participant id: the `member` of its reports, and the
    /// name a targeted flush or an addressed output uses for this call.
    member: String,
    control: Option<Arc<SessionControl>>,
}

impl CallAudioSender {
    pub fn new(
        outbound: RtpOutboundMediaSession,
        member: impl Into<String>,
        control: Option<Arc<SessionControl>>,
    ) -> Self {
        Self {
            outbound,
            member: member.into(),
            control,
        }
    }

    /// The call leg's participant id.
    pub fn member(&self) -> &str {
        &self.member
    }

    /// The RTP session, for audio that is not a pipeline output.
    pub fn outbound_mut(&mut self) -> &mut RtpOutboundMediaSession {
        &mut self.outbound
    }

    /// Queue one pipeline output for this call.
    ///
    /// - An output addressed (`participant.to`) to other members is skipped.
    /// - Audio is queued under the turn and chunk in its metadata, or
    ///   untagged without them.
    /// - A `speech_end` Json output marks the end of its chunk.
    /// - Anything else is ignored.
    ///
    /// Rejected audio drops everything queued, as the RTP session requires.
    pub fn push_output(&mut self, output: TransportData) -> Result<()> {
        if !participant::is_for(&output.metadata, &self.member) {
            return Ok(());
        }
        let tag = TurnTag::from_metadata(&output.metadata);
        match output.data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => {
                let queued = self.outbound.queue_audio_tagged(
                    samples.into_shared(),
                    sample_rate,
                    channels,
                    tag,
                );
                if queued.is_err() {
                    self.flush_all();
                }
                queued
            }
            RuntimeData::Json(value)
                if value.get("kind").and_then(|kind| kind.as_str()) == Some("speech_end") =>
            {
                self.outbound.mark_chunk_end(tag);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// The datagram to send now, if a whole frame is queued. Call once per
    /// frame duration and send what it returns. Publishes the reports that
    /// became due.
    pub fn next_datagram(&mut self) -> Option<Vec<u8>> {
        let datagram = match self.outbound.next_audio_datagram() {
            Ok(datagram) => datagram,
            Err(error) => {
                warn!("RTP output rejected for {}: {}", self.member, error);
                self.outbound.clear_pending_audio();
                None
            }
        };
        self.publish_reports();
        datagram
    }

    /// Apply a session's targeted flush if it names this call (or nobody).
    /// Returns the samples dropped, `None` when the flush is for others.
    pub fn apply_flush(&mut self, target: &FlushTarget) -> Option<usize> {
        let dropped = self.outbound.apply_flush(target, &self.member);
        self.publish_reports();
        dropped
    }

    /// The session-wide flush: drop everything queued, of every turn.
    pub fn flush_all(&mut self) {
        self.outbound.clear_pending_audio();
        self.publish_reports();
    }

    fn publish_reports(&mut self) {
        let reports = self.outbound.take_release_reports(&self.member);
        if let Some(control) = &self.control {
            for report in reports {
                control.publish_audio_released(report);
            }
        }
    }
}

/// The media of one answered call, ready to [`run`](Self::run).
pub struct CallMedia {
    pub call_id: String,
    pub stream: Box<dyn StreamSession>,
    pub rtp_socket: Arc<UdpSocket>,
    pub local_rtp_addr: SocketAddr,
    /// Where RTP is sent until the caller's first packet shows its source.
    pub remote_rtp_addr: SocketAddr,
    pub inbound: RtpMediaSession,
    pub sender: CallAudioSender,
    /// One RTP packet is sent per `frame_duration`.
    pub frame_duration: Duration,
    /// Targeted flush requests of the session (`AcceptedCall::flush_requests`).
    pub flush_requests: Option<mpsc::Receiver<FlushTarget>>,
    /// Session-wide flush requests (`AcceptedCall::session_flushes`).
    pub session_flushes: Option<broadcast::Receiver<()>>,
    /// Silence frames sent first, to open NAT bindings.
    pub nat_punch_frames: usize,
    /// Held so the RTP port stays allocated while the call runs.
    _rtp_port: Option<RtpPortLease>,
}

impl CallMedia {
    /// The media of an accepted call. Send `accepted.response` to the caller
    /// before or after; this does not.
    pub fn from_accepted(accepted: AcceptedCall) -> Self {
        let AcceptedCall {
            copies,
            response: _,
            stream,
            rtp_socket,
            local_rtp_addr,
            remote_rtp_addr,
            negotiated_audio,
            codec_map,
            call_id,
            leg_id,
            session_control,
            flush_requests,
            session_flushes,
            rtp_port,
        } = accepted;
        let inbound = RtpMediaSession::new(
            call_id.clone(),
            leg_id.clone(),
            ParticipantRole::User,
            codec_map,
            JITTER_DEPTH_PACKETS,
        )
        .with_copy_context(copies.clone());
        let (outbound, ptime_ms) = match RtpOutboundMediaSession::new(
            negotiated_audio.codec,
            negotiated_audio.payload_type,
            OUTBOUND_SSRC,
            1,
            0,
            negotiated_audio.ptime_ms,
        ) {
            Ok(outbound) => (outbound, negotiated_audio.ptime_ms),
            Err(error) => {
                warn!(
                    "outbound RTP for call {} falls back to PCMU/20ms: {}",
                    call_id, error
                );
                let fallback =
                    RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, OUTBOUND_SSRC, 1, 0, 20)
                        .expect("PCMU 20 ms outbound RTP session");
                (fallback, 20)
            }
        };
        Self {
            call_id,
            stream,
            rtp_socket: Arc::new(rtp_socket),
            local_rtp_addr,
            remote_rtp_addr,
            inbound,
            sender: CallAudioSender::new(
                outbound.with_copy_context(copies),
                leg_id,
                session_control,
            ),
            frame_duration: Duration::from_millis(u64::from(ptime_ms)),
            flush_requests,
            session_flushes,
            nat_punch_frames: NAT_PUNCH_FRAMES,
            _rtp_port: Some(rtp_port),
        }
    }

    /// Media built by hand, for a host that does not use
    /// [`AcceptedCall`] (and for tests).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        call_id: impl Into<String>,
        stream: Box<dyn StreamSession>,
        rtp_socket: Arc<UdpSocket>,
        remote_rtp_addr: SocketAddr,
        inbound: RtpMediaSession,
        sender: CallAudioSender,
        frame_duration: Duration,
    ) -> Self {
        let local_rtp_addr = rtp_socket
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        Self {
            call_id: call_id.into(),
            stream,
            rtp_socket,
            local_rtp_addr,
            remote_rtp_addr,
            inbound,
            sender,
            frame_duration,
            flush_requests: None,
            session_flushes: None,
            nat_punch_frames: 0,
            _rtp_port: None,
        }
    }

    /// Run the call's media until `shutdown` completes, the pipeline session
    /// ends or the RTP socket fails; then close the stream.
    ///
    /// Outbound audio is paced: one packet per frame duration while audio is
    /// queued, nothing while it is not.
    pub async fn run<F: Future>(mut self, shutdown: F) {
        tokio::pin!(shutdown);
        let call_id = self.call_id.clone();
        let mut remote_rtp_addr = self.remote_rtp_addr;
        let mut latched = false;
        let mut buf = vec![0u8; 1500];

        if self.nat_punch_frames > 0 {
            info!(
                "RTP NAT punching: {} silence frames to {} for call {}",
                self.nat_punch_frames, remote_rtp_addr, call_id
            );
            let silence = vec![0.0f32; self.sender.outbound.samples_per_frame()];
            for _ in 0..self.nat_punch_frames {
                match self.sender.outbound.packetize_audio_datagram(&silence) {
                    Ok(datagram) => {
                        let _ = self.rtp_socket.send_to(&datagram, remote_rtp_addr).await;
                    }
                    Err(error) => warn!("RTP silence rejected for call {}: {}", call_id, error),
                }
                tokio::time::sleep(self.frame_duration).await;
            }
        }

        let mut pace = tokio::time::interval(self.frame_duration);
        // A packet counts as released when it is taken, so it must leave at
        // its pace: a tick missed by a whole period is skipped, never made
        // up in a burst, and the ticks stay on the 20 ms grid.
        pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("shutdown of media loop for call {}", call_id);
                    break;
                }
                target = next_flush(&mut self.flush_requests) => {
                    if let Some(dropped) = self.sender.apply_flush(&target) {
                        debug!(
                            "call {} flushed {} queued samples of turn {}",
                            call_id, dropped, target.turn_id
                        );
                    }
                }
                _ = next_session_flush(&mut self.session_flushes) => {
                    self.sender.flush_all();
                }
                _ = pace.tick() => {
                    if let Some(datagram) = self.sender.next_datagram() {
                        let _ = self.rtp_socket.send_to(&datagram, remote_rtp_addr).await;
                    }
                }
                output = self.stream.recv_output() => {
                    match output {
                        Ok(Some(output)) => {
                            if let Err(error) = self.sender.push_output(output) {
                                warn!("RTP PCM input rejected for call {}: {}", call_id, error);
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                received = self.rtp_socket.recv_from(&mut buf) => {
                    let (len, mut src) = match received {
                        Ok(received) => received,
                        // Windows reports an ICMP port-unreachable for a
                        // packet we sent (the caller's port is not open
                        // yet) as a failed receive. The socket is fine.
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                            debug!("RTP port unreachable for call {}: {}", call_id, error);
                            continue;
                        }
                        Err(error) => {
                            warn!("RTP recv error for call {}: {}", call_id, error);
                            break;
                        }
                    };
                    let mut packet = std::mem::replace(&mut buf, vec![0; 1500]);
                    packet.truncate(len);
                    loop {
                        if !latched {
                            info!("RTP latching: call {} to remote RTP source {}", call_id, src);
                            remote_rtp_addr = src;
                            latched = true;
                            // The caller's media is flowing: it can hear
                            // output even if its ACK never reached us.
                            if let Some(control) = &self.sender.control {
                                control.publish_media_ready(self.sender.member(), "telephony", &call_id);
                            }
                        }
                        if src.ip() == remote_rtp_addr.ip() {
                            match self.inbound.receive_owned_datagram(packet.into()) {
                                Ok(frames) => {
                                    for frame in frames {
                                        if let Err(error) = self.stream.send_input(frame).await {
                                            warn!("pipeline input failed for call {}: {}", call_id, error);
                                            break;
                                        }
                                    }
                                }
                                Err(error) => debug!("RTP decode error for call {}: {}", call_id, error),
                            }
                        }
                        // Take what else is waiting, so the socket does not back up.
                        match self.rtp_socket.try_recv_from(&mut buf) {
                            Ok((next_len, next_src)) => {
                                src = next_src;
                                packet = std::mem::replace(&mut buf, vec![0; 1500]);
                                packet.truncate(next_len);
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }

        if let Err(error) = self.stream.close().await {
            warn!("closing pipeline stream of call {}: {}", call_id, error);
        }
        info!("media loop ended for call {}", call_id);
    }
}

/// The next targeted flush; never resolves without a receiver.
async fn next_flush(requests: &mut Option<mpsc::Receiver<FlushTarget>>) -> FlushTarget {
    loop {
        match requests {
            Some(receiver) => match receiver.recv().await {
                Some(target) => return target,
                None => *requests = None,
            },
            None => std::future::pending::<()>().await,
        }
    }
}

/// The next session-wide flush; never resolves without a receiver.
async fn next_session_flush(flushes: &mut Option<broadcast::Receiver<()>>) {
    loop {
        match flushes {
            Some(receiver) => match receiver.recv().await {
                // Missed requests are one flush: everything queued goes.
                Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => return,
                Err(broadcast::error::RecvError::Closed) => *flushes = None,
            },
            None => std::future::pending::<()>().await,
        }
    }
}
