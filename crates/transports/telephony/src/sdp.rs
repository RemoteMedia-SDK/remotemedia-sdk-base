//! SDP negotiation primitives.

use crate::{AudioCodec, Error, Result, TelephonyTransportConfig};
use std::collections::HashMap;

/// Negotiated media parameters for one RTP audio stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedAudio {
    /// Selected codec.
    pub codec: AudioCodec,
    /// RTP payload type selected for the codec.
    pub payload_type: u8,
    /// RTP clock rate.
    pub clock_rate_hz: u32,
    /// Packetization time in milliseconds.
    pub ptime_ms: u16,
}

impl NegotiatedAudio {
    /// Create a negotiated audio description for a codec/payload pair.
    pub fn new(codec: AudioCodec, payload_type: u8, ptime_ms: u16) -> Self {
        Self {
            codec,
            payload_type,
            clock_rate_hz: codec.clock_rate_hz(),
            ptime_ms,
        }
    }
}

/// SDP audio media offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdpAudioOffer {
    /// Remote RTP host.
    pub connection_address: String,
    /// Remote RTP port.
    pub port: u16,
    /// Offered RTP payload types.
    pub payload_types: Vec<u8>,
    /// RTP payload mappings.
    pub rtpmap: HashMap<u8, AudioCodec>,
    /// Packetization time in milliseconds.
    pub ptime_ms: Option<u16>,
    /// SDP media direction.
    pub direction: MediaDirection,
}

/// SDP media direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDirection {
    /// Send and receive media.
    SendRecv,
    /// Receive only.
    RecvOnly,
    /// Send only.
    SendOnly,
    /// Inactive.
    Inactive,
}

impl Default for MediaDirection {
    fn default() -> Self {
        Self::SendRecv
    }
}

/// Parse the first audio media offer from an SDP body.
pub fn parse_audio_offer(sdp: &str) -> Result<SdpAudioOffer> {
    let mut session_connection: Option<String> = None;
    let mut media_connection: Option<String> = None;
    let mut audio_port: Option<u16> = None;
    let mut payload_types = Vec::new();
    let mut rtpmap = HashMap::new();
    let mut ptime_ms = None;
    let mut direction = MediaDirection::SendRecv;
    let mut in_audio = false;

    for raw_line in sdp.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("c=") {
            let address = parse_connection_address(rest)?;
            if in_audio {
                media_connection = Some(address);
            } else {
                session_connection = Some(address);
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("m=") {
            in_audio = false;
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("audio") {
                in_audio = true;
                audio_port = Some(parts[1].parse::<u16>().map_err(|e| {
                    Error::Sdp(format!("invalid audio media port '{}': {e}", parts[1]))
                })?);
                payload_types = parts[3..]
                    .iter()
                    .map(|pt| {
                        pt.parse::<u8>()
                            .map_err(|e| Error::Sdp(format!("invalid payload type '{pt}': {e}")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for payload_type in &payload_types {
                    match *payload_type {
                        0 => {
                            rtpmap.insert(0, AudioCodec::Pcmu);
                        }
                        8 => {
                            rtpmap.insert(8, AudioCodec::Pcma);
                        }
                        _ => {}
                    }
                }
            }
            continue;
        }

        if !in_audio {
            continue;
        }

        if let Some(rest) = line.strip_prefix("a=rtpmap:") {
            match parse_rtpmap(rest) {
                Ok((pt, codec)) => {
                    rtpmap.insert(pt, codec);
                }
                Err(Error::Sdp(message)) if message.starts_with("unsupported rtpmap codec") => {}
                Err(e) => return Err(e),
            }
        } else if let Some(rest) = line.strip_prefix("a=ptime:") {
            ptime_ms = Some(
                rest.parse::<u16>()
                    .map_err(|e| Error::Sdp(format!("invalid ptime '{rest}': {e}")))?,
            );
        } else {
            direction = match line {
                "a=sendrecv" => MediaDirection::SendRecv,
                "a=recvonly" => MediaDirection::RecvOnly,
                "a=sendonly" => MediaDirection::SendOnly,
                "a=inactive" => MediaDirection::Inactive,
                _ => direction,
            };
        }
    }

    let connection_address = media_connection
        .or(session_connection)
        .ok_or_else(|| Error::Sdp("missing SDP connection address".to_string()))?;
    let port = audio_port.ok_or_else(|| Error::Sdp("missing SDP audio m-line".to_string()))?;

    Ok(SdpAudioOffer {
        connection_address,
        port,
        payload_types,
        rtpmap,
        ptime_ms,
        direction,
    })
}

/// Negotiate an audio codec from an offer and transport preferences.
pub fn negotiate_audio(
    offer: &SdpAudioOffer,
    config: &TelephonyTransportConfig,
) -> Result<NegotiatedAudio> {
    for preferred in &config.codec_preferences {
        for payload_type in &offer.payload_types {
            if offer.rtpmap.get(payload_type) == Some(preferred) {
                return Ok(NegotiatedAudio::new(
                    *preferred,
                    *payload_type,
                    offer.ptime_ms.unwrap_or(config.frame_duration_ms),
                ));
            }
        }
    }

    Err(Error::Sdp(
        "no compatible audio codec in SDP offer".to_string(),
    ))
}

/// Build a minimal SDP answer for a negotiated audio stream.
pub fn build_audio_answer(
    media_address: &str,
    media_port: u16,
    negotiated: &NegotiatedAudio,
) -> String {
    let codec_name = match negotiated.codec {
        AudioCodec::Pcmu => "PCMU",
        AudioCodec::Pcma => "PCMA",
        AudioCodec::Opus => "opus",
    };
    format!(
        "v=0\r\n\
         o=remotemedia 0 0 IN IP4 {media_address}\r\n\
         s=RemoteMedia Telephony\r\n\
         c=IN IP4 {media_address}\r\n\
         t=0 0\r\n\
         m=audio {media_port} RTP/AVP {payload_type}\r\n\
         a=rtpmap:{payload_type} {codec_name}/{clock_rate}\r\n\
         a=ptime:{ptime}\r\n\
         a=sendrecv\r\n",
        payload_type = negotiated.payload_type,
        clock_rate = negotiated.clock_rate_hz,
        ptime = negotiated.ptime_ms
    )
}

fn parse_connection_address(value: &str) -> Result<String> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(Error::Sdp(format!("invalid connection line: c={value}")));
    }
    Ok(parts[2].to_string())
}

fn parse_rtpmap(value: &str) -> Result<(u8, AudioCodec)> {
    let (payload, encoding) = value
        .split_once(char::is_whitespace)
        .ok_or_else(|| Error::Sdp(format!("invalid rtpmap attribute: {value}")))?;
    let payload_type = payload
        .parse::<u8>()
        .map_err(|e| Error::Sdp(format!("invalid rtpmap payload type '{payload}': {e}")))?;
    let codec_name = encoding
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let codec = match codec_name.as_str() {
        "pcmu" => AudioCodec::Pcmu,
        "pcma" => AudioCodec::Pcma,
        "opus" => AudioCodec::Opus,
        _ => {
            return Err(Error::Sdp(format!(
                "unsupported rtpmap codec '{codec_name}'"
            )));
        }
    };
    Ok((payload_type, codec))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFER: &str = "v=0\r\n\
o=alice 1 1 IN IP4 192.0.2.10\r\n\
s=-\r\n\
c=IN IP4 192.0.2.10\r\n\
t=0 0\r\n\
m=audio 49170 RTP/AVP 0 8 111\r\n\
a=rtpmap:111 opus/48000/2\r\n\
a=ptime:20\r\n\
a=sendrecv\r\n";

    const TWILIO_OFFER: &str = "v=0\r\n\
o=root 338139175 338139175 IN IP4 172.18.161.199\r\n\
s=Twilio Media Gateway\r\n\
c=IN IP4 168.86.138.173\r\n\
t=0 0\r\n\
m=audio 13924 RTP/AVP 0 8 101\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=fmtp:101 0-16\r\n\
a=ptime:20\r\n\
a=maxptime:20\r\n\
a=sendrecv\r\n";

    #[test]
    fn parses_audio_offer() {
        let offer = parse_audio_offer(OFFER).unwrap();
        assert_eq!(offer.connection_address, "192.0.2.10");
        assert_eq!(offer.port, 49170);
        assert_eq!(offer.rtpmap.get(&0), Some(&AudioCodec::Pcmu));
        assert_eq!(offer.rtpmap.get(&111), Some(&AudioCodec::Opus));
        assert_eq!(offer.ptime_ms, Some(20));
    }

    #[test]
    fn ignores_unsupported_auxiliary_rtpmap_payloads() {
        let offer = parse_audio_offer(TWILIO_OFFER).unwrap();
        assert_eq!(offer.connection_address, "168.86.138.173");
        assert_eq!(offer.port, 13924);
        assert_eq!(offer.payload_types, vec![0, 8, 101]);
        assert_eq!(offer.rtpmap.get(&0), Some(&AudioCodec::Pcmu));
        assert_eq!(offer.rtpmap.get(&8), Some(&AudioCodec::Pcma));
        assert_eq!(offer.rtpmap.get(&101), None);
    }

    #[test]
    fn negotiates_preferred_codec() {
        let offer = parse_audio_offer(OFFER).unwrap();
        let negotiated = negotiate_audio(&offer, &TelephonyTransportConfig::default()).unwrap();
        assert_eq!(negotiated.codec, AudioCodec::Opus);
        assert_eq!(negotiated.payload_type, 111);
    }

    #[test]
    fn builds_answer() {
        let answer = build_audio_answer(
            "198.51.100.2",
            20_000,
            &NegotiatedAudio::new(AudioCodec::Pcmu, 0, 20),
        );
        assert!(answer.contains("m=audio 20000 RTP/AVP 0"));
        assert!(answer.contains("a=rtpmap:0 PCMU/8000"));
    }
}
