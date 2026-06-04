//! Conference media routing primitives.

use crate::session::CallLegId;
use std::collections::{BTreeSet, HashMap};

/// A routing decision for one outbound audio frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRoute {
    /// Source leg that produced the media.
    pub source: CallLegId,
    /// Destination legs that should receive the media.
    pub destinations: Vec<CallLegId>,
}

/// In-memory conference routing table.
#[derive(Debug, Clone, Default)]
pub struct ConferenceRouter {
    legs: BTreeSet<CallLegId>,
    muted: BTreeSet<CallLegId>,
    explicit_routes: HashMap<CallLegId, BTreeSet<CallLegId>>,
}

impl ConferenceRouter {
    /// Add a call leg to the conference.
    pub fn add_leg(&mut self, leg: impl Into<CallLegId>) {
        self.legs.insert(leg.into());
    }

    /// Mute or unmute a leg as a media destination.
    pub fn set_muted(&mut self, leg: impl Into<CallLegId>, muted: bool) {
        let leg = leg.into();
        if muted {
            self.muted.insert(leg);
        } else {
            self.muted.remove(&leg);
        }
    }

    /// Configure explicit destinations for a source leg.
    pub fn set_route(
        &mut self,
        source: impl Into<CallLegId>,
        destinations: impl IntoIterator<Item = CallLegId>,
    ) {
        self.explicit_routes
            .insert(source.into(), destinations.into_iter().collect());
    }

    /// Route media from one source leg to destination legs.
    pub fn route_from(&self, source: &str) -> MediaRoute {
        let destinations = self
            .explicit_routes
            .get(source)
            .cloned()
            .unwrap_or_else(|| {
                self.legs
                    .iter()
                    .filter(|leg| leg.as_str() != source)
                    .cloned()
                    .collect()
            })
            .into_iter()
            .filter(|leg| !self.muted.contains(leg))
            .collect();

        MediaRoute {
            source: source.to_string(),
            destinations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_to_other_unmuted_legs() {
        let mut router = ConferenceRouter::default();
        router.add_leg("user");
        router.add_leg("client");
        router.add_leg("bot");
        router.set_muted("client", true);
        let route = router.route_from("bot");
        assert_eq!(route.destinations, vec!["user".to_string()]);
    }

    #[test]
    fn honors_explicit_routes() {
        let mut router = ConferenceRouter::default();
        router.add_leg("user");
        router.add_leg("client");
        router.add_leg("bot");
        router.set_route("bot", vec!["client".to_string()]);
        let route = router.route_from("bot");
        assert_eq!(route.destinations, vec!["client".to_string()]);
    }
}
