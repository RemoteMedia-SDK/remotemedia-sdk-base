//! SIP/RTP telephony transport for RemoteMedia pipelines.
//!
//! This crate owns telephony network lifecycle: SIP signaling, SDP
//! negotiation, RTP sockets, jitter handling, SIPREC association, and
//! conference media routing. Pipeline nodes receive and emit decoded media
//! frames through `TransportData`; they do not bind SIP or RTP sockets.

#![warn(clippy::all)]

pub mod codec;
pub mod conference;
pub mod config;
pub mod error;
pub mod hermes;
pub mod jitter;
pub mod metrics;
pub mod plugin;
pub mod rtp;
pub mod sdp;
pub mod session;
pub mod sip;
pub mod siprec;
pub mod transport;

pub use config::{
    AudioCodec, ConferenceConfig, HermesToolConfig, JitterConfig, SipAccessMode,
    SipRateLimitConfig, TelephonyTransportConfig,
};
pub use error::{Error, Result};
pub use hermes::{CallControlCommand, HermesToolState, TelephonyHermesEvent};
pub use metrics::{CallMetrics, RtpCounters};
pub use plugin::TelephonyTransportPlugin;
pub use rtp::{RtpMediaSession, RtpOutboundMediaSession, RtpPacket};
pub use sdp::NegotiatedAudio;
pub use session::{CallDirection, CallId, CallLegId, CallSessionState, ParticipantRole};
pub use sip::{
    build_method_not_allowed, build_options_ok, build_response, extract_raw_method,
    extract_via_from_raw, parse_request, SipMethod, SipRequest, SipTransactionResponse,
    SUPPORTED_METHODS,
};
pub use transport::{AcceptedCall, TelephonyTransport};

/// Get the version of this crate.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_available() {
        assert!(!crate::version().is_empty());
    }
}
