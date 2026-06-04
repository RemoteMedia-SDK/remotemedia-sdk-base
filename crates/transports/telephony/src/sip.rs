//! SIP signaling primitives.
//!
//! Full parser/dialog handling is implemented in later tasks. This module is
//! intentionally present now so the crate layout matches the approved
//! transport-oriented OpenSpec design.

use crate::{Error, Result};
use std::collections::HashMap;

/// SIP methods required for the initial gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SipMethod {
    /// Start or modify a call session.
    Invite,
    /// Confirm final response to INVITE.
    Ack,
    /// Terminate an established dialog.
    Bye,
    /// Cancel a pending INVITE.
    Cancel,
    /// Capability keepalive/probe.
    Options,
}

/// Comma-separated list of supported SIP methods for the `Allow` header.
pub const SUPPORTED_METHODS: &str = "INVITE, ACK, BYE, CANCEL, OPTIONS";

impl SipMethod {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "INVITE" => Some(Self::Invite),
            "ACK" => Some(Self::Ack),
            "BYE" => Some(Self::Bye),
            "CANCEL" => Some(Self::Cancel),
            "OPTIONS" => Some(Self::Options),
            _ => None,
        }
    }

    /// Return the wire method token.
    pub fn as_str(self) -> &'static str {
        match self {
            SipMethod::Invite => "INVITE",
            SipMethod::Ack => "ACK",
            SipMethod::Bye => "BYE",
            SipMethod::Cancel => "CANCEL",
            SipMethod::Options => "OPTIONS",
        }
    }
}

/// Parsed SIP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SipRequest {
    /// SIP method.
    pub method: SipMethod,
    /// Request URI.
    pub uri: String,
    /// SIP version.
    pub version: String,
    /// Header map with lowercase keys.
    pub headers: HashMap<String, String>,
    /// All Via headers in wire order. SIP responses must echo every Via.
    pub via_headers: Vec<String>,
    /// Message body.
    pub body: String,
}

/// SIP transaction response produced by the telephony transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SipTransactionResponse {
    /// Status code returned to the peer.
    pub status_code: u16,
    /// Wire-format response bytes.
    pub bytes: Vec<u8>,
}

impl SipRequest {
    /// Return a header by case-insensitive name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// SIP Call-ID header.
    pub fn call_id(&self) -> Option<&str> {
        self.header("call-id").or_else(|| self.header("i"))
    }
}

/// Parse a SIP request from bytes.
pub fn parse_request(bytes: &[u8]) -> Result<SipRequest> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::Sip(format!("SIP message is not UTF-8: {e}")))?;
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .unwrap_or((text, ""));
    let mut lines = head.lines().map(str::trim_end);
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Sip("missing SIP request line".to_string()))?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(Error::Sip(format!(
            "invalid SIP request line: {request_line}"
        )));
    }
    let method = SipMethod::parse(parts[0])
        .ok_or_else(|| Error::Sip(format!("unsupported SIP method '{}'", parts[0])))?;

    let mut headers = HashMap::new();
    let mut via_headers: Vec<String> = Vec::new();
    let mut current_key: Option<String> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }

        if line.starts_with(' ') || line.starts_with('\t') {
            let continuation = line.trim();
            if let Some(key) = &current_key {
                headers.entry(key.clone()).and_modify(|value: &mut String| {
                    value.push(' ');
                    value.push_str(continuation);
                });
                if key == "via" {
                    if let Some(value) = via_headers.last_mut() {
                        value.push(' ');
                        value.push_str(continuation);
                    }
                }
            }
            continue;
        }

        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| Error::Sip(format!("invalid SIP header line: {line}")))?;
        let key = normalize_header_name(name);
        current_key = Some(key.clone());
        let value = value.trim().to_string();
        if key == "via" {
            via_headers.push(value.clone());
        }
        headers.insert(key, value);
    }

    Ok(SipRequest {
        method,
        uri: parts[1].to_string(),
        version: parts[2].to_string(),
        headers,
        via_headers,
        body: body.to_string(),
    })
}

/// Build a SIP response reusing dialog headers from a request.
pub fn build_response(
    request: &SipRequest,
    status_code: u16,
    reason: &str,
    body: &str,
    contact_host: Option<&str>,
) -> String {
    let mut response = format!("SIP/2.0 {status_code} {reason}\r\n");
    for via in &request.via_headers {
        response.push_str(&format!("Via: {via}\r\n"));
    }
    for header in ["from", "to", "call-id", "cseq"] {
        if let Some(value) = request.header(header) {
            let name = canonical_header_name(header);
            let value = if header == "to"
                && status_code != 100
                && !value.to_ascii_lowercase().contains("tag=")
            {
                format!("{value};tag=remotemedia")
            } else {
                value.to_string()
            };
            response.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    if request.method == SipMethod::Invite && (200..300).contains(&status_code) {
        let host = contact_host
            .or_else(|| request.uri.split('@').nth(1))
            .unwrap_or("127.0.0.1");
        response.push_str(&format!(
            "Contact: <sip:remotemedia@{};transport=udp>\r\n",
            host
        ));
    }
    response.push_str("Server: RemoteMedia Telephony\r\n");
    response.push_str(&format!("Content-Length: {}\r\n", body.len()));
    if !body.is_empty() {
        response.push_str("Content-Type: application/sdp\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    response
}

/// Build a minimal SIP OPTIONS 200 OK response.
pub fn build_options_ok(request: &SipRequest) -> String {
    let mut response = build_response(request, 200, "OK", "", None);
    response = response.replace(
        "Content-Length: 0\r\n",
        "Allow: INVITE, ACK, BYE, CANCEL, OPTIONS\r\nAccept: application/sdp\r\nContent-Length: 0\r\n",
    );
    response
}

fn normalize_header_name(name: &str) -> String {
    match name.trim().to_ascii_lowercase().as_str() {
        "i" => "call-id".to_string(),
        "f" => "from".to_string(),
        "t" => "to".to_string(),
        "v" => "via".to_string(),
        "m" => "contact".to_string(),
        other => other.to_string(),
    }
}

fn canonical_header_name(name: &str) -> &'static str {
    match name {
        "via" => "Via",
        "from" => "From",
        "to" => "To",
        "call-id" => "Call-ID",
        "cseq" => "CSeq",
        _ => "X-Unknown",
    }
}

/// Extract the SIP method from raw datagram bytes without full parsing.
///
/// Scans the first `max_scan_bytes` bytes to find the method token in the
/// request line. Returns `None` if the datagram is too short or the method
/// cannot be extracted.
pub fn extract_raw_method(datagram: &[u8]) -> Option<&str> {
    // Only scan the first 32 bytes — the method token is always at the start.
    let scan_limit = datagram.len().min(32);
    let text = std::str::from_utf8(&datagram[..scan_limit]).ok()?;

    // Method is the first whitespace-delimited token.
    let method_end = text.find(|c: char| c.is_ascii_whitespace())?;
    let method = &text[..method_end];

    // Method must be all ASCII letters (A-Z).
    if method.is_empty() || !method.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }

    Some(method)
}

/// Build a `405 Method Not Allowed` response for an unsupported SIP method.
///
/// Extracts the Via header from the raw bytes to route the response back,
/// and includes an `Allow` header listing the supported methods.
pub fn build_method_not_allowed(datagram: &[u8]) -> SipTransactionResponse {
    // Try to extract the Via header for routing.
    let via = extract_via_from_raw(datagram);

    let mut response = String::from("SIP/2.0 405 Method Not Allowed\r\n");

    if let Some(v) = via {
        response.push_str(&format!("Via: {v}\r\n"));
    }

    // Add minimal dialog headers if we can find them.
    if let Some(from) = extract_header_from_raw(datagram, b"From:") {
        response.push_str(&format!("From: {from}\r\n"));
    }
    if let Some(to) = extract_header_from_raw(datagram, b"To:") {
        response.push_str(&format!("To: {to};tag=remotemedia\r\n"));
    }
    if let Some(call_id) = extract_header_from_raw(datagram, b"Call-ID:") {
        response.push_str(&format!("Call-ID: {call_id}\r\n"));
    }
    if let Some(cseq) = extract_header_from_raw(datagram, b"CSeq:") {
        response.push_str(&format!("CSeq: {cseq}\r\n"));
    }

    response.push_str(&format!("Allow: {SUPPORTED_METHODS}\r\n"));
    response.push_str("Server: RemoteMedia Telephony\r\n");
    response.push_str("Content-Length: 0\r\n");
    response.push_str("\r\n");

    SipTransactionResponse {
        status_code: 405,
        bytes: response.into_bytes(),
    }
}

/// Extract a single header value from raw SIP bytes.
fn extract_header_from_raw(datagram: &[u8], header_name: &[u8]) -> Option<String> {
    let header_prefix = header_name.to_ascii_lowercase();
    let text = std::str::from_utf8(datagram).ok()?;
    for line in text.lines() {
        let line_lower = line.trim_end().to_ascii_lowercase();
        let header_prefix_str = String::from_utf8_lossy(&header_prefix);
        if line_lower.starts_with(header_prefix_str.as_ref()) {
            let value = line[header_name.len()..].trim().to_string();
            return Some(value);
        }
    }
    None
}

/// Extract the first Via header from raw SIP bytes.
pub fn extract_via_from_raw(datagram: &[u8]) -> Option<String> {
    extract_header_from_raw(datagram, b"Via:")
}

#[cfg(test)]
mod tests {
    use super::*;

    const INVITE: &str = "INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 192.0.2.10:5060;branch=z9hG4bK1\r\n\
From: <sip:alice@example.com>;tag=abc\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-123\r\n\
CSeq: 1 INVITE\r\n\
Contact: <sip:alice@192.0.2.10>\r\n\
Content-Type: application/sdp\r\n\
Content-Length: 3\r\n\
\r\n\
abc";

    const PROXIED_INVITE: &str = "INVITE sip:bot@example.com SIP/2.0\r\n\
Via: SIP/2.0/UDP 198.51.100.20:5060;branch=z9hG4bKproxy\r\n\
Via: SIP/2.0/UDP 192.0.2.10:5060;rport=5060;branch=z9hG4bKclient\r\n\
From: <sip:alice@example.com>;tag=abc\r\n\
To: <sip:bot@example.com>\r\n\
Call-ID: call-456\r\n\
CSeq: 2 INVITE\r\n\
Content-Length: 0\r\n\
\r\n";

    #[test]
    fn parses_invite() {
        let request = parse_request(INVITE.as_bytes()).unwrap();
        assert_eq!(request.method, SipMethod::Invite);
        assert_eq!(request.uri, "sip:bot@example.com");
        assert_eq!(request.call_id(), Some("call-123"));
        assert_eq!(
            request.via_headers,
            vec!["SIP/2.0/UDP 192.0.2.10:5060;branch=z9hG4bK1"]
        );
        assert_eq!(request.body, "abc");
    }

    #[test]
    fn builds_response_with_dialog_headers() {
        let request = parse_request(INVITE.as_bytes()).unwrap();
        let response = build_response(&request, 200, "OK", "sdp", None);
        assert!(response.starts_with("SIP/2.0 200 OK"));
        assert!(response.contains("Via: SIP/2.0/UDP 192.0.2.10:5060"));
        assert!(response.contains("To: <sip:bot@example.com>;tag=remotemedia"));
        assert!(response.contains("Contact: <sip:remotemedia@example.com;transport=udp>"));
        assert!(response.contains("Content-Type: application/sdp"));
    }

    #[test]
    fn builds_response_with_all_via_headers_in_order() {
        let request = parse_request(PROXIED_INVITE.as_bytes()).unwrap();
        assert_eq!(
            request.via_headers,
            vec![
                "SIP/2.0/UDP 198.51.100.20:5060;branch=z9hG4bKproxy",
                "SIP/2.0/UDP 192.0.2.10:5060;rport=5060;branch=z9hG4bKclient"
            ]
        );

        let response = build_response(&request, 200, "OK", "sdp", None);
        let first = response
            .find("Via: SIP/2.0/UDP 198.51.100.20:5060;branch=z9hG4bKproxy")
            .unwrap();
        let second = response
            .find("Via: SIP/2.0/UDP 192.0.2.10:5060;rport=5060;branch=z9hG4bKclient")
            .unwrap();
        assert!(first < second);
    }

    #[test]
    fn builds_trying_without_to_tag() {
        let request = parse_request(INVITE.as_bytes()).unwrap();
        let response = build_response(&request, 100, "Trying", "", None);
        assert!(response.starts_with("SIP/2.0 100 Trying"));
        assert!(response.contains("To: <sip:bot@example.com>\r\n"));
        assert!(!response.contains("tag=remotemedia"));
        assert!(!response.contains("Contact:"));
    }

    #[test]
    fn method_round_trips_name() {
        assert_eq!(SipMethod::Invite.as_str(), "INVITE");
    }

    #[test]
    fn extract_raw_method_from_invite() {
        assert_eq!(extract_raw_method(INVITE.as_bytes()), Some("INVITE"));
    }

    #[test]
    fn extract_raw_method_from_register() {
        let register = b"REGISTER sip:example.com SIP/2.0\r\n";
        assert_eq!(extract_raw_method(register), Some("REGISTER"));
    }

    #[test]
    fn extract_raw_method_from_subscribe() {
        let subscribe = b"SUBSCRIBE sip:example.com SIP/2.0\r\n";
        assert_eq!(extract_raw_method(subscribe), Some("SUBSCRIBE"));
    }

    #[test]
    fn extract_raw_method_none_for_short_datagram() {
        assert_eq!(extract_raw_method(b"REG"), None);
    }

    #[test]
    fn extract_raw_method_none_for_non_ascii() {
        assert_eq!(extract_raw_method(&[0xFF, 0xFE, 0x00, 0x00]), None);
    }

    #[test]
    fn build_method_not_allowed_has_405_and_allow_header() {
        let register = "REGISTER sip:example.com SIP/2.0\r\n\r\n\r\n";
        let response = build_method_not_allowed(register.as_bytes());
        assert_eq!(response.status_code, 405);
        let body = std::str::from_utf8(&response.bytes).unwrap();
        assert!(body.starts_with("SIP/2.0 405 Method Not Allowed"));
        assert!(body.contains(&format!("Allow: {SUPPORTED_METHODS}")));
    }
}
