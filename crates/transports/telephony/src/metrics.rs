//! Telephony transport metrics primitives.

/// Snapshot of per-call RTP counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RtpCounters {
    /// RTP packets received from the network.
    pub packets_received: u64,
    /// RTP packets sent to the network.
    pub packets_sent: u64,
    /// Packets dropped by validation or jitter handling.
    pub packets_dropped: u64,
    /// Packet loss concealment frames generated.
    pub plc_frames: u64,
}

impl RtpCounters {
    /// Record an inbound RTP packet.
    pub fn record_received(&mut self) {
        self.packets_received += 1;
    }

    /// Record an outbound RTP packet.
    pub fn record_sent(&mut self) {
        self.packets_sent += 1;
    }

    /// Record a dropped RTP packet.
    pub fn record_dropped(&mut self) {
        self.packets_dropped += 1;
    }

    /// Record a generated PLC frame.
    pub fn record_plc(&mut self) {
        self.plc_frames += 1;
    }
}

/// High-level call metrics snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CallMetrics {
    /// Active call count.
    pub active_calls: u64,
    /// Completed calls.
    pub completed_calls: u64,
    /// Failed calls.
    pub failed_calls: u64,
    /// RTP counters across calls represented by this snapshot.
    pub rtp: RtpCounters,
}

impl CallMetrics {
    /// Record a call start.
    pub fn record_call_started(&mut self) {
        self.active_calls += 1;
    }

    /// Record a normally completed call.
    pub fn record_call_completed(&mut self) {
        self.active_calls = self.active_calls.saturating_sub(1);
        self.completed_calls += 1;
    }

    /// Record a failed call.
    pub fn record_call_failed(&mut self) {
        self.active_calls = self.active_calls.saturating_sub(1);
        self.failed_calls += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_counters() {
        let mut metrics = CallMetrics::default();
        metrics.record_call_started();
        metrics.rtp.record_received();
        metrics.rtp.record_dropped();
        metrics.record_call_completed();

        assert_eq!(metrics.active_calls, 0);
        assert_eq!(metrics.completed_calls, 1);
        assert_eq!(metrics.rtp.packets_received, 1);
        assert_eq!(metrics.rtp.packets_dropped, 1);
    }
}
