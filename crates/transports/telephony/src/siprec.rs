//! SIPREC metadata primitives.

use crate::{Error, Result};

/// Participant in a SIPREC mirrored call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedParticipant {
    /// Stable participant identifier from SIPREC metadata.
    pub id: String,
    /// Human-readable role or label when available.
    pub role: Option<String>,
}

/// Parsed SIPREC metadata needed for channel association.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SiprecMetadata {
    /// Recorded participants.
    pub participants: Vec<RecordedParticipant>,
    /// Stream-to-participant associations.
    pub stream_participants: Vec<RecordedStreamAssociation>,
}

/// Mapping from a recorded media stream to a participant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedStreamAssociation {
    /// SIPREC stream identifier.
    pub stream_id: String,
    /// SIPREC participant identifier.
    pub participant_id: String,
}

/// Return true when SIP headers identify a SIPREC request.
pub fn is_siprec_request(content_type: Option<&str>, require_header: Option<&str>) -> bool {
    content_type
        .map(|ct| {
            let ct = ct.to_ascii_lowercase();
            ct.contains("application/rs-metadata") || ct.contains("application/rs-metadata+xml")
        })
        .unwrap_or(false)
        || require_header
            .map(|value| value.to_ascii_lowercase().contains("siprec"))
            .unwrap_or(false)
}

/// Parse a small SIPREC metadata subset.
///
/// This intentionally extracts only stable IDs and associations needed by the
/// transport. A full XML model can replace this without changing callers.
pub fn parse_siprec_metadata(xml: &str) -> Result<SiprecMetadata> {
    let mut metadata = SiprecMetadata::default();

    for participant in find_elements(xml, "participant") {
        if let Some(id) = attr(participant, "participant_id").or_else(|| attr(participant, "id")) {
            metadata.participants.push(RecordedParticipant {
                id,
                role: attr(participant, "role"),
            });
        }
    }

    for stream in find_elements(xml, "stream") {
        let stream_id = attr(stream, "stream_id").or_else(|| attr(stream, "id"));
        let participant_id = attr(stream, "participant_id").or_else(|| attr(stream, "participant"));
        if let (Some(stream_id), Some(participant_id)) = (stream_id, participant_id) {
            metadata
                .stream_participants
                .push(RecordedStreamAssociation {
                    stream_id,
                    participant_id,
                });
        }
    }

    if metadata.participants.is_empty() && metadata.stream_participants.is_empty() {
        return Err(Error::Sip(
            "SIPREC metadata contained no supported associations".to_string(),
        ));
    }

    Ok(metadata)
}

fn find_elements<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let needle = format!("<{name}");
    let mut rest = xml;
    while let Some(start) = rest.find(&needle) {
        rest = &rest[start..];
        if let Some(end) = rest.find('>') {
            out.push(&rest[..=end]);
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    out
}

fn attr(element: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let start = element.find(&needle)? + needle.len();
    let tail = &element[start..];
    let end = tail.find('"')?;
    Some(tail[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_siprec_headers() {
        assert!(is_siprec_request(Some("application/rs-metadata+xml"), None));
        assert!(is_siprec_request(None, Some("siprec")));
        assert!(!is_siprec_request(Some("application/sdp"), None));
    }

    #[test]
    fn parses_metadata_subset() {
        let metadata = parse_siprec_metadata(
            r#"<recording>
<participant participant_id="p1" role="caller"/>
<participant participant_id="p2" role="agent"/>
<stream stream_id="s1" participant_id="p1"/>
</recording>"#,
        )
        .unwrap();
        assert_eq!(metadata.participants.len(), 2);
        assert_eq!(metadata.stream_participants[0].stream_id, "s1");
        assert_eq!(metadata.stream_participants[0].participant_id, "p1");
    }
}
