//! Jitter-buffer primitives.

use crate::rtp::{sequence_is_newer, RtpPacket};
use std::collections::{BTreeMap, HashSet};

/// Decision produced for an RTP packet after jitter-buffer admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterDecision {
    /// Packet was accepted and may later be emitted in order.
    Accepted,
    /// Packet arrived too late to be useful.
    Late,
    /// Packet is a duplicate of one already observed.
    Duplicate,
}

/// Minimal sequence-oriented jitter buffer for RTP packets.
#[derive(Debug, Clone)]
pub struct JitterBuffer {
    expected: Option<u16>,
    max_depth_packets: usize,
    buffered: BTreeMap<u16, RtpPacket>,
    seen: HashSet<u16>,
}

impl JitterBuffer {
    /// Create a jitter buffer with a maximum number of queued packets.
    pub fn new(max_depth_packets: usize) -> Self {
        Self {
            expected: None,
            max_depth_packets: max_depth_packets.max(1),
            buffered: BTreeMap::new(),
            seen: HashSet::new(),
        }
    }

    /// Admit one packet into the buffer.
    pub fn push(&mut self, packet: RtpPacket) -> JitterDecision {
        let sequence = packet.info.sequence;
        if self.seen.contains(&sequence) || self.buffered.contains_key(&sequence) {
            return JitterDecision::Duplicate;
        }

        if let Some(expected) = self.expected {
            if sequence != expected && !sequence_is_newer(sequence, expected) {
                return JitterDecision::Late;
            }
        } else {
            self.expected = Some(sequence);
        }

        self.buffered.insert(sequence, packet);
        JitterDecision::Accepted
    }

    /// Pop the next in-order packet if available.
    pub fn pop_ready(&mut self) -> Option<RtpPacket> {
        let expected = self.expected?;
        let packet = self.buffered.remove(&expected)?;
        self.seen.insert(expected);
        self.expected = Some(expected.wrapping_add(1));
        Some(packet)
    }

    /// Pop the next packet, skipping a missing sequence if the buffer is too deep.
    pub fn pop_or_skip_missing(&mut self) -> Option<RtpPacket> {
        if let Some(packet) = self.pop_ready() {
            return Some(packet);
        }

        if self.buffered.len() < self.max_depth_packets {
            return None;
        }

        let expected = self.expected?;
        self.seen.insert(expected);
        self.expected = Some(expected.wrapping_add(1));
        self.pop_ready()
    }

    /// Skip one missing sequence if the buffer is too deep.
    pub fn skip_missing_if_needed(&mut self) -> Option<u16> {
        if self.buffered.len() < self.max_depth_packets {
            return None;
        }

        let missing = self.expected?;
        self.seen.insert(missing);
        self.expected = Some(missing.wrapping_add(1));
        Some(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtp::{RtpPacket, RtpPacketInfo};

    fn pkt(sequence: u16) -> RtpPacket {
        RtpPacket {
            info: RtpPacketInfo {
                payload_type: 0,
                sequence,
                timestamp: u32::from(sequence) * 160,
                ssrc: 1,
                marker: false,
            },
            payload: vec![sequence as u8],
        }
    }

    #[test]
    fn reorders_packets() {
        let mut jitter = JitterBuffer::new(3);
        assert_eq!(jitter.push(pkt(10)), JitterDecision::Accepted);
        assert_eq!(jitter.push(pkt(12)), JitterDecision::Accepted);
        assert_eq!(jitter.push(pkt(11)), JitterDecision::Accepted);
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 10);
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 11);
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 12);
    }

    #[test]
    fn skips_missing_when_buffer_is_full() {
        let mut jitter = JitterBuffer::new(2);
        jitter.push(pkt(1));
        jitter.push(pkt(3));
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 1);
        jitter.push(pkt(4));
        assert_eq!(jitter.skip_missing_if_needed(), Some(2));
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 3);
    }

    #[test]
    fn rejects_duplicates_and_late_packets() {
        let mut jitter = JitterBuffer::new(2);
        assert_eq!(jitter.push(pkt(1)), JitterDecision::Accepted);
        assert_eq!(jitter.push(pkt(1)), JitterDecision::Duplicate);
        assert_eq!(jitter.pop_ready().unwrap().info.sequence, 1);
        assert_eq!(jitter.push(pkt(1)), JitterDecision::Duplicate);
        assert_eq!(jitter.push(pkt(0)), JitterDecision::Late);
    }
}
