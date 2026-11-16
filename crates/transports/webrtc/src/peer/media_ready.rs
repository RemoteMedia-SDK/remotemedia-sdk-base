//! When a peer can hear output (openspec: add-ai-sdk-voice-library, 2.1).

/// What changed for a peer's ability to hear output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaChange {
    /// Connected with its outbound audio bound: publish `media_ready`.
    Ready,
    /// The connection went away: publish `media_lost`.
    Lost,
}

/// Turns observations of a peer connection into at most one `Ready` per
/// connected period: once when it is connected with audio bound, and again
/// only after it was lost and came back.
#[derive(Debug, Default)]
pub(crate) struct MediaReadyGate {
    ready: bool,
}

impl MediaReadyGate {
    pub(crate) fn observe(&mut self, connected: bool, audio_bound: bool) -> Option<MediaChange> {
        let ready = connected && audio_bound;
        if ready == self.ready {
            return None;
        }
        self.ready = ready;
        Some(if ready {
            MediaChange::Ready
        } else {
            MediaChange::Lost
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_once_per_connected_period_and_again_after_reconnect() {
        let mut gate = MediaReadyGate::default();
        // Connected alone is not enough: the audio track must be bound.
        assert_eq!(gate.observe(true, false), None);
        assert_eq!(gate.observe(false, true), None);
        assert_eq!(gate.observe(true, true), Some(MediaChange::Ready));
        // Once, however often it is observed.
        assert_eq!(gate.observe(true, true), None);
        assert_eq!(gate.observe(true, true), None);
        // Disconnected, then connected again.
        assert_eq!(gate.observe(false, true), Some(MediaChange::Lost));
        assert_eq!(gate.observe(false, true), None);
        assert_eq!(gate.observe(true, true), Some(MediaChange::Ready));
    }
}
