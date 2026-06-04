# RemoteMedia Telephony Transport

`remotemedia-telephony` owns SIP/RTP network lifecycle outside the pipeline
node graph. SIP dialogs, SDP negotiation, RTP ports, jitter handling, SIPREC
association, and conference routing stay in the transport. Pipelines receive
decoded audio frames and emit audio for RTP packetization.

## Operational Notes

- Bind SIP on `sip_bind_address`; use `advertised_media_address` when the
  gateway is behind NAT or an SBC.
- Open the configured RTP UDP port range in the firewall.
- Use `allowed_peers` for SBC/PBX allow-listing.
- Keep trunk credentials outside manifests; inject them through deployment
  configuration or secret stores.
- TLS/SRTP fields are intentionally left as future-compatible config
  boundaries; the initial gateway supports SIP/RTP UDP.

## Transport Boundary

Conference routing is transport-internal by default so echo suppression,
injected bot audio, SIPREC passive mode, and teardown stay tied to call legs.
Graph nodes remain responsible for STT, TTS, tool dispatch, splitting, mixing
utilities, and audio transformation.

## Hermes Boundary

Hermes receives call events containing call ID, participant role, transcription
context, and tool-call state. Hermes may send call-control commands such as
inspect, stop, join, inject response, mute, and unmute. The telephony transport
turns those commands into call-leg routing or teardown actions.
