//! RTP media primitives.

use crate::codec::{decode_g711, encode_g711, CodecMap, OpusCodec};
use crate::jitter::{JitterBuffer, JitterDecision};
use crate::session::{apply_frame_metadata, CallId, CallLegId, ParticipantRole};
use crate::{AudioCodec, Error, Result};
use remotemedia_core::data::{AudioSamples, RuntimeData};
use remotemedia_core::transport::TransportData;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

/// RTP packet metadata tracked before packet payload decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpPacketInfo {
    /// RTP payload type.
    pub payload_type: u8,
    /// RTP sequence number.
    pub sequence: u16,
    /// RTP timestamp.
    pub timestamp: u32,
    /// RTP synchronization source.
    pub ssrc: u32,
    /// Marker bit.
    pub marker: bool,
}

/// Parsed RTP packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpPacket {
    /// Packet metadata.
    pub info: RtpPacketInfo,
    /// RTP payload bytes after header, CSRC list, and optional extension.
    pub payload: Vec<u8>,
}

/// Stateful RTP packetizer for one outbound media stream.
#[derive(Debug, Clone)]
pub struct RtpPacketizer {
    payload_type: u8,
    ssrc: u32,
    next_sequence: u16,
    next_timestamp: u32,
    samples_per_frame: u32,
    mark_next: bool,
}

impl RtpPacketizer {
    /// Create a packetizer for fixed-cadence audio frames.
    pub fn new(
        payload_type: u8,
        ssrc: u32,
        initial_sequence: u16,
        initial_timestamp: u32,
        samples_per_frame: u32,
    ) -> Result<Self> {
        if payload_type > 127 {
            return Err(Error::Rtp(format!(
                "RTP payload type must fit in 7 bits, got {payload_type}"
            )));
        }

        if samples_per_frame == 0 {
            return Err(Error::Rtp(
                "samples_per_frame must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            payload_type,
            ssrc,
            next_sequence: initial_sequence,
            next_timestamp: initial_timestamp,
            samples_per_frame,
            mark_next: true,
        })
    }

    /// Packetize one encoded audio frame and advance RTP sequence/timestamp state.
    pub fn packetize(&mut self, payload: Vec<u8>) -> RtpPacket {
        let packet = RtpPacket {
            info: RtpPacketInfo {
                payload_type: self.payload_type,
                sequence: self.next_sequence,
                timestamp: self.next_timestamp,
                ssrc: self.ssrc,
                marker: self.mark_next,
            },
            payload,
        };

        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.next_timestamp = self.next_timestamp.wrapping_add(self.samples_per_frame);
        self.mark_next = false;
        packet
    }

    /// Return the sequence number that will be used for the next packet.
    pub fn next_sequence(&self) -> u16 {
        self.next_sequence
    }

    /// Return the RTP timestamp that will be used for the next packet.
    pub fn next_timestamp(&self) -> u32 {
        self.next_timestamp
    }
}

impl RtpPacket {
    /// Parse an RTP packet from a UDP datagram.
    pub fn parse(datagram: &[u8]) -> Result<Self> {
        if datagram.len() < 12 {
            return Err(Error::Rtp(format!(
                "RTP packet too short: {} bytes",
                datagram.len()
            )));
        }

        let version = datagram[0] >> 6;
        if version != 2 {
            return Err(Error::Rtp(format!("unsupported RTP version: {version}")));
        }

        let has_extension = datagram[0] & 0x10 != 0;
        let csrc_count = usize::from(datagram[0] & 0x0f);
        let marker = datagram[1] & 0x80 != 0;
        let payload_type = datagram[1] & 0x7f;
        let sequence = u16::from_be_bytes([datagram[2], datagram[3]]);
        let timestamp = u32::from_be_bytes([datagram[4], datagram[5], datagram[6], datagram[7]]);
        let ssrc = u32::from_be_bytes([datagram[8], datagram[9], datagram[10], datagram[11]]);

        let mut payload_offset = 12 + (csrc_count * 4);
        if datagram.len() < payload_offset {
            return Err(Error::Rtp(format!(
                "RTP packet too short for {csrc_count} CSRC entries"
            )));
        }

        if has_extension {
            if datagram.len() < payload_offset + 4 {
                return Err(Error::Rtp(
                    "RTP packet too short for extension header".to_string(),
                ));
            }

            let extension_len_words = usize::from(u16::from_be_bytes([
                datagram[payload_offset + 2],
                datagram[payload_offset + 3],
            ]));
            payload_offset += 4 + (extension_len_words * 4);
            if datagram.len() < payload_offset {
                return Err(Error::Rtp(format!(
                    "RTP packet too short for extension payload of {extension_len_words} words"
                )));
            }
        }

        Ok(Self {
            info: RtpPacketInfo {
                payload_type,
                sequence,
                timestamp,
                ssrc,
                marker,
            },
            payload: datagram[payload_offset..].to_vec(),
        })
    }

    /// Serialize this packet as RTP version 2 without CSRC or extension headers.
    pub fn write(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.payload.len());
        out.push(0x80);
        out.push(self.info.payload_type | if self.info.marker { 0x80 } else { 0 });
        out.extend_from_slice(&self.info.sequence.to_be_bytes());
        out.extend_from_slice(&self.info.timestamp.to_be_bytes());
        out.extend_from_slice(&self.info.ssrc.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

/// Returns true when `candidate` is newer than `reference` in RTP sequence space.
pub fn sequence_is_newer(candidate: u16, reference: u16) -> bool {
    let delta = candidate.wrapping_sub(reference);
    delta != 0 && delta < 0x8000
}

/// Allocates even RTP ports from a configured range.
#[derive(Debug, Clone)]
pub struct RtpPortAllocator {
    start: u16,
    end: u16,
    bind_ip: IpAddr,
    allocated: Arc<Mutex<BTreeSet<u16>>>,
}

impl RtpPortAllocator {
    /// Create a new allocator for an inclusive port range.
    pub fn new(start: u16, end: u16) -> Result<Self> {
        if start == 0 || end == 0 || start > end {
            return Err(Error::Rtp(format!("invalid RTP port range {start}-{end}")));
        }

        Ok(Self {
            start,
            end,
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            allocated: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    /// Allocate the next available even RTP port.
    pub fn allocate(&self) -> Result<RtpPortLease> {
        let mut allocated = self
            .allocated
            .lock()
            .map_err(|_| Error::Rtp("RTP port allocator lock poisoned".to_string()))?;
        for port in self.start..=self.end {
            if port % 2 != 0 || allocated.contains(&port) {
                continue;
            }
            allocated.insert(port);
            return Ok(RtpPortLease {
                port,
                bind_ip: self.bind_ip,
                allocated: self.allocated.clone(),
            });
        }

        Err(Error::Rtp(format!(
            "no available RTP ports in range {}-{}",
            self.start, self.end
        )))
    }
}

/// RAII lease for an allocated RTP port.
#[derive(Debug)]
pub struct RtpPortLease {
    port: u16,
    bind_ip: IpAddr,
    allocated: Arc<Mutex<BTreeSet<u16>>>,
}

/// RTP media session for one inbound audio stream.
#[derive(Debug)]
pub struct RtpMediaSession {
    call_id: CallId,
    leg_id: CallLegId,
    participant_role: ParticipantRole,
    codec_map: CodecMap,
    jitter: JitterBuffer,
    opus: Option<OpusCodec>,
    plc_payload_type: u8,
    plc_sample_rate_hz: u32,
    plc_samples_per_frame: usize,
}

impl RtpMediaSession {
    /// Create a new inbound RTP media session.
    pub fn new(
        call_id: CallId,
        leg_id: CallLegId,
        participant_role: ParticipantRole,
        codec_map: CodecMap,
        jitter_depth_packets: usize,
    ) -> Self {
        let plc_payload = codec_map
            .get(0)
            .or_else(|| codec_map.get(8))
            .or_else(|| codec_map.get(111))
            .cloned();
        let plc_sample_rate_hz = plc_payload
            .as_ref()
            .map(|payload| payload.clock_rate_hz)
            .unwrap_or(8_000);
        let plc_samples_per_frame = (plc_sample_rate_hz / 50) as usize;
        Self {
            call_id,
            leg_id,
            participant_role,
            codec_map,
            jitter: JitterBuffer::new(jitter_depth_packets),
            opus: OpusCodec::new(48_000, 1).ok(),
            plc_payload_type: plc_payload.map(|payload| payload.payload_type).unwrap_or(0),
            plc_sample_rate_hz,
            plc_samples_per_frame,
        }
    }

    /// Admit an RTP datagram and return any in-order decoded audio frames.
    pub fn receive_datagram(&mut self, datagram: &[u8]) -> Result<Vec<TransportData>> {
        let packet = RtpPacket::parse(datagram)?;
        match self.jitter.push(packet) {
            JitterDecision::Accepted => {}
            JitterDecision::Duplicate | JitterDecision::Late => return Ok(Vec::new()),
        }

        let mut frames = Vec::new();
        while let Some(packet) = self.jitter.pop_ready() {
            frames.push(self.packet_to_transport_data(packet)?);
        }
        if let Some(missing_sequence) = self.jitter.skip_missing_if_needed() {
            frames.push(self.plc_transport_data(missing_sequence));
            while let Some(packet) = self.jitter.pop_ready() {
                frames.push(self.packet_to_transport_data(packet)?);
            }
        }
        Ok(frames)
    }

    fn plc_transport_data(&self, sequence: u16) -> TransportData {
        let data = TransportData::new(RuntimeData::Audio {
            samples: AudioSamples::from(vec![0.0; self.plc_samples_per_frame]),
            sample_rate: self.plc_sample_rate_hz,
            channels: 1,
            stream_id: Some(self.leg_id.clone()),
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: Some(serde_json::json!({ "plc": true })),
        })
        .with_sequence(u64::from(sequence))
        .with_metadata("telephony.plc".to_string(), "true".to_string());

        apply_frame_metadata(
            data,
            &self.call_id,
            &self.leg_id,
            self.participant_role,
            self.codec_map
                .get(self.plc_payload_type)
                .map(|payload| match payload.codec {
                    AudioCodec::Pcmu => "pcmu",
                    AudioCodec::Pcma => "pcma",
                    AudioCodec::Opus => "opus",
                })
                .unwrap_or("unknown"),
            self.plc_sample_rate_hz,
            None,
            Some(sequence),
        )
    }

    fn packet_to_transport_data(&mut self, packet: RtpPacket) -> Result<TransportData> {
        let payload = self
            .codec_map
            .get(packet.info.payload_type)
            .ok_or_else(|| {
                Error::Rtp(format!(
                    "unknown RTP payload type {} for call {}",
                    packet.info.payload_type, self.call_id
                ))
            })?;

        let samples = match payload.codec {
            AudioCodec::Pcmu | AudioCodec::Pcma => decode_g711(payload.codec, &packet.payload)?,
            AudioCodec::Opus => self
                .opus
                .as_mut()
                .ok_or_else(|| Error::Codec("Opus decoder is unavailable".to_string()))
                .and_then(|codec| codec.decode(&packet.payload))?,
        };

        let data = TransportData::new(RuntimeData::Audio {
            samples: AudioSamples::from(samples),
            sample_rate: payload.clock_rate_hz,
            channels: 1,
            stream_id: Some(self.leg_id.clone()),
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        })
        .with_sequence(u64::from(packet.info.sequence));

        Ok(apply_frame_metadata(
            data,
            &self.call_id,
            &self.leg_id,
            self.participant_role,
            match payload.codec {
                AudioCodec::Pcmu => "pcmu",
                AudioCodec::Pcma => "pcma",
                AudioCodec::Opus => "opus",
            },
            payload.clock_rate_hz,
            Some(packet.info.timestamp),
            Some(packet.info.sequence),
        ))
    }
}

/// RTP media session for outbound audio packets.
#[derive(Debug)]
pub struct RtpOutboundMediaSession {
    codec: AudioCodec,
    packetizer: RtpPacketizer,
    opus: Option<OpusCodec>,
    expected_samples_per_frame: usize,
}

impl RtpOutboundMediaSession {
    /// Create an outbound RTP media session.
    pub fn new(
        codec: AudioCodec,
        payload_type: u8,
        ssrc: u32,
        initial_sequence: u16,
        initial_timestamp: u32,
        frame_duration_ms: u16,
    ) -> Result<Self> {
        let sample_rate = codec.clock_rate_hz();
        let samples_per_frame = (sample_rate / 1000) * u32::from(frame_duration_ms);
        Ok(Self {
            codec,
            packetizer: RtpPacketizer::new(
                payload_type,
                ssrc,
                initial_sequence,
                initial_timestamp,
                samples_per_frame,
            )?,
            opus: if codec == AudioCodec::Opus {
                Some(OpusCodec::new(sample_rate, 1)?)
            } else {
                None
            },
            expected_samples_per_frame: samples_per_frame as usize,
        })
    }

    /// Encode and packetize one f32 PCM audio frame.
    pub fn packetize_audio_frame(&mut self, samples: &[f32]) -> Result<RtpPacket> {
        if samples.len() != self.expected_samples_per_frame {
            return Err(Error::Rtp(format!(
                "expected {} samples for 20ms {:?} frame, got {}",
                self.expected_samples_per_frame,
                self.codec,
                samples.len()
            )));
        }
        let payload = match self.codec {
            AudioCodec::Pcmu | AudioCodec::Pcma => encode_g711(self.codec, samples)?,
            AudioCodec::Opus => self
                .opus
                .as_mut()
                .ok_or_else(|| Error::Codec("Opus encoder is unavailable".to_string()))?
                .encode(samples)?,
        };
        Ok(self.packetizer.packetize(payload))
    }
}

impl RtpPortLease {
    /// Allocated UDP port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Socket address for binding this RTP port.
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.port)
    }
}

impl Drop for RtpPortLease {
    fn drop(&mut self) {
        if let Ok(mut allocated) = self.allocated.lock() {
            allocated.remove(&self.port);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_packet() {
        let packet = RtpPacket::parse(&[
            0x80,
            0x80 | 111,
            0x12,
            0x34,
            0x01,
            0x02,
            0x03,
            0x04,
            0xaa,
            0xbb,
            0xcc,
            0xdd,
            1,
            2,
            3,
        ])
        .unwrap();

        assert_eq!(packet.info.payload_type, 111);
        assert!(packet.info.marker);
        assert_eq!(packet.info.sequence, 0x1234);
        assert_eq!(packet.info.timestamp, 0x0102_0304);
        assert_eq!(packet.info.ssrc, 0xaabb_ccdd);
        assert_eq!(packet.payload, vec![1, 2, 3]);
    }

    #[test]
    fn writes_packet() {
        let packet = RtpPacket {
            info: RtpPacketInfo {
                payload_type: 0,
                sequence: 7,
                timestamp: 160,
                ssrc: 42,
                marker: false,
            },
            payload: vec![9, 8, 7],
        };

        let encoded = packet.write();
        let reparsed = RtpPacket::parse(&encoded).unwrap();

        assert_eq!(reparsed, packet);
    }

    #[test]
    fn skips_csrc_and_extension() {
        let packet = RtpPacket::parse(&[
            0x90 | 1,
            111,
            0,
            1,
            0,
            0,
            0,
            2,
            0,
            0,
            0,
            3, // header with extension + 1 CSRC
            0xde,
            0xad,
            0xbe,
            0xef, // CSRC
            0x10,
            0x00,
            0x00,
            0x01, // extension profile + 1 32-bit word
            0xaa,
            0xbb,
            0xcc,
            0xdd, // extension payload
            0x44,
            0x55, // RTP payload
        ])
        .unwrap();

        assert_eq!(packet.info.payload_type, 111);
        assert_eq!(packet.payload, vec![0x44, 0x55]);
    }

    #[test]
    fn rejects_bad_version() {
        let err = RtpPacket::parse(&[0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err();
        assert!(matches!(err, Error::Rtp(_)));
    }

    #[test]
    fn compares_sequence_with_rollover() {
        assert!(sequence_is_newer(2, 1));
        assert!(sequence_is_newer(0, u16::MAX));
        assert!(!sequence_is_newer(u16::MAX, 0));
        assert!(!sequence_is_newer(5, 5));
    }

    #[test]
    fn packetizer_advances_sequence_and_timestamp() {
        let mut packetizer = RtpPacketizer::new(111, 99, u16::MAX, u32::MAX - 479, 480).unwrap();

        let first = packetizer.packetize(vec![1]);
        let second = packetizer.packetize(vec![2]);

        assert_eq!(first.info.sequence, u16::MAX);
        assert_eq!(first.info.timestamp, u32::MAX - 479);
        assert!(first.info.marker);
        assert_eq!(second.info.sequence, 0);
        assert_eq!(second.info.timestamp, 0);
        assert!(!second.info.marker);
        assert_eq!(packetizer.next_sequence(), 1);
        assert_eq!(packetizer.next_timestamp(), 480);
    }

    #[test]
    fn port_allocator_leases_even_ports_and_releases_on_drop() {
        let allocator = RtpPortAllocator::new(10_000, 10_003).unwrap();
        let a = allocator.allocate().unwrap();
        let b = allocator.allocate().unwrap();
        assert_eq!(a.port(), 10_000);
        assert_eq!(b.port(), 10_002);
        assert!(allocator.allocate().is_err());
        drop(a);
        let c = allocator.allocate().unwrap();
        assert_eq!(c.port(), 10_000);
    }

    #[test]
    fn media_session_decodes_g711_to_transport_data() {
        let mut codec_map = CodecMap::default();
        codec_map.insert(0, AudioCodec::Pcmu).unwrap();
        let mut session = RtpMediaSession::new(
            "call".into(),
            "caller".into(),
            ParticipantRole::User,
            codec_map,
            3,
        );
        let packet = RtpPacket {
            info: RtpPacketInfo {
                payload_type: 0,
                sequence: 7,
                timestamp: 1120,
                ssrc: 9,
                marker: false,
            },
            payload: vec![0xff; 160],
        };

        let frames = session.receive_datagram(&packet.write()).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].sequence, Some(7));
        assert_eq!(
            frames[0].metadata.get("telephony.rtp_timestamp").unwrap(),
            "1120"
        );
        match &frames[0].data {
            RuntimeData::Audio {
                sample_rate,
                channels,
                ..
            } => {
                assert_eq!(*sample_rate, 8_000);
                assert_eq!(*channels, 1);
            }
            _ => panic!("expected audio"),
        }
    }

    #[test]
    fn outbound_session_packetizes_g711_audio() {
        let mut session =
            RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 44, 1, 160, 20).unwrap();
        let packet = session.packetize_audio_frame(&vec![0.0; 160]).unwrap();
        assert_eq!(packet.info.payload_type, 0);
        assert_eq!(packet.info.sequence, 1);
        assert_eq!(packet.info.timestamp, 160);
        assert_eq!(packet.payload.len(), 160);
    }

    #[test]
    fn outbound_session_packetizes_opus_audio() {
        let mut session =
            RtpOutboundMediaSession::new(AudioCodec::Opus, 111, 44, 1, 960, 20).unwrap();
        let packet = session.packetize_audio_frame(&vec![0.0; 960]).unwrap();
        assert_eq!(packet.info.payload_type, 111);
        assert_eq!(packet.info.timestamp, 960);
        assert!(!packet.payload.is_empty());
    }

    #[test]
    fn outbound_session_rejects_wrong_frame_size() {
        let mut session =
            RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 44, 1, 160, 20).unwrap();
        assert!(session.packetize_audio_frame(&vec![0.0; 80]).is_err());
    }

    #[test]
    fn media_session_emits_plc_frame_for_missing_packet() {
        let mut codec_map = CodecMap::default();
        codec_map.insert(0, AudioCodec::Pcmu).unwrap();
        let mut session = RtpMediaSession::new(
            "call".into(),
            "caller".into(),
            ParticipantRole::User,
            codec_map,
            2,
        );
        let mut frames = Vec::new();
        for sequence in [1_u16, 3, 4] {
            let packet = RtpPacket {
                info: RtpPacketInfo {
                    payload_type: 0,
                    sequence,
                    timestamp: u32::from(sequence) * 160,
                    ssrc: 9,
                    marker: false,
                },
                payload: vec![0xff; 160],
            };
            frames.extend(session.receive_datagram(&packet.write()).unwrap());
        }
        let packet = RtpPacket {
            info: RtpPacketInfo {
                payload_type: 0,
                sequence: 5,
                timestamp: 800,
                ssrc: 9,
                marker: false,
            },
            payload: vec![0xff; 160],
        };
        frames.extend(session.receive_datagram(&packet.write()).unwrap());
        assert!(frames
            .iter()
            .any(|frame| frame.metadata.get("telephony.plc").is_some()));
    }
}
