//! Single-use admission tickets for a host that admits members to sessions
//! (openspec `add-ai-sdk-voice-library`, tasks 4.7 and 6.2).
//!
//! The host (the application's backend, in-process or over the runtime's
//! control API) decides who may join which session and mints a ticket for
//! that member. The browser presents the ticket in `peer.announce`; the
//! identity, name, metadata and control grants it joins with are the ones
//! stored here, never what the browser says about itself.
//!
//! This module is plain Rust with no signalling dependency, so the rules
//! (valid once, valid for a limited time, bound to one identity) are tested
//! without a signalling server. One implementation serves the embedded Node
//! server (`WebRtcAgentServer`) and `remotemedia serve` ([`crate::agent_host`]).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// The control operations a grant may name (Entry's vocabulary,
/// `docs/tokens.md` in remotemedia-scaling).
pub const GRANT_OPS: [&str; 5] = ["tap", "publish", "rpc", "intercept", "node_state"];

/// One control grant: an operation on a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketGrant {
    pub op: String,
    pub node: String,
    /// `tap` only: just the output produced from the member's own input.
    pub own: bool,
    /// `rpc` only: the methods the member may call.
    pub methods: Vec<String>,
}

/// Who a ticket admits, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    /// The room (hosted session) the member joins.
    pub room: String,
    /// The member's identity (`participant.id`).
    pub member: String,
    pub name: Option<String>,
    pub metadata: HashMap<String, String>,
    /// Control grants. Empty: media and the pipeline data channel only.
    pub grants: Vec<TicketGrant>,
}

/// Why a ticket request or a ticket was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketError {
    /// No ticket was presented.
    Missing,
    /// The ticket was never issued, was already used, or was revoked.
    Unknown,
    /// The ticket's lifetime ran out before it was used.
    Expired,
    /// The admission is not one a ticket can be minted for.
    Invalid(String),
}

impl std::fmt::Display for TicketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TicketError::Missing => {
                write!(
                    f,
                    "a ticket is required: ask the application's backend to admit you"
                )
            }
            TicketError::Unknown => write!(f, "the ticket is unknown or was already used"),
            TicketError::Expired => write!(f, "the ticket has expired"),
            TicketError::Invalid(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for TicketError {}

/// Check an admission before a ticket is minted for it.
pub fn validate(admission: &Admission, ttl: Duration) -> Result<(), TicketError> {
    if admission.member.is_empty() {
        return Err(TicketError::Invalid("the member id is empty".to_string()));
    }
    if admission.room.is_empty() {
        return Err(TicketError::Invalid("the session is empty".to_string()));
    }
    if ttl.is_zero() {
        return Err(TicketError::Invalid(
            "the ticket lifetime must be at least one second".to_string(),
        ));
    }
    for grant in &admission.grants {
        if !GRANT_OPS.contains(&grant.op.as_str()) {
            return Err(TicketError::Invalid(format!(
                "unknown grant op '{}'; expected one of {}",
                grant.op,
                GRANT_OPS.join(", ")
            )));
        }
        if grant.node.is_empty() {
            return Err(TicketError::Invalid(format!(
                "a '{}' grant names no node",
                grant.op
            )));
        }
    }
    Ok(())
}

struct Held {
    admission: Admission,
    expires: SystemTime,
}

/// Tickets that were minted and not used yet.
#[derive(Default)]
pub struct TicketStore {
    pending: Mutex<HashMap<String, Held>>,
}

impl TicketStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `admission` for the holder of `ticket` until `now + ttl`. Returns
    /// when the ticket stops being accepted. Tickets that expired unused are
    /// forgotten here, so the table stays as small as the admissions in
    /// flight.
    pub fn issue(
        &self,
        ticket: String,
        admission: Admission,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<SystemTime, TicketError> {
        validate(&admission, ttl)?;
        if ticket.is_empty() {
            return Err(TicketError::Invalid("the ticket is empty".to_string()));
        }
        let expires = now + ttl;
        let mut pending = self.pending.lock().expect("tickets");
        pending.retain(|_, held| held.expires > now);
        pending.insert(ticket, Held { admission, expires });
        Ok(expires)
    }

    /// The admission of `ticket`, consumed: a ticket is valid once, even
    /// when it is refused for having expired.
    pub fn redeem(&self, ticket: Option<&str>, now: SystemTime) -> Result<Admission, TicketError> {
        let ticket = ticket
            .filter(|ticket| !ticket.is_empty())
            .ok_or(TicketError::Missing)?;
        let held = self
            .pending
            .lock()
            .expect("tickets")
            .remove(ticket)
            .ok_or(TicketError::Unknown)?;
        if held.expires <= now {
            return Err(TicketError::Expired);
        }
        Ok(held.admission)
    }

    /// Forget the unused tickets of `member` in `room`. Returns how many.
    pub fn revoke_member(&self, room: &str, member: &str) -> usize {
        self.revoke(|admission| admission.room == room && admission.member == member)
    }

    /// Forget every unused ticket of `room`. Returns how many.
    pub fn revoke_room(&self, room: &str) -> usize {
        self.revoke(|admission| admission.room == room)
    }

    fn revoke(&self, matches: impl Fn(&Admission) -> bool) -> usize {
        let mut pending = self.pending.lock().expect("tickets");
        let before = pending.len();
        pending.retain(|_, held| !matches(&held.admission));
        before - pending.len()
    }

    /// How many tickets of `room` could still be used at `now`.
    pub fn pending_for_room(&self, room: &str, now: SystemTime) -> usize {
        self.pending
            .lock()
            .expect("tickets")
            .values()
            .filter(|held| held.admission.room == room && held.expires > now)
            .count()
    }
}

/// The room a `(pipeline, session key)` pair is served as. The pipeline
/// name's length is part of it, so no two pairs share a room whatever
/// characters the names contain.
pub fn room_key(pipeline: &str, session_key: &str) -> String {
    format!("{}:{}:{}", pipeline.len(), pipeline, session_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(room: &str, member: &str) -> Admission {
        Admission {
            room: room.to_string(),
            member: member.to_string(),
            name: Some("Ada".to_string()),
            metadata: HashMap::from([("role".to_string(), "host".to_string())]),
            grants: Vec::new(),
        }
    }

    const TTL: Duration = Duration::from_secs(30);

    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000)
    }

    #[test]
    fn a_ticket_admits_the_identity_it_was_minted_for() {
        let store = TicketStore::new();
        store
            .issue("t-1".into(), admission("room", "user-17"), t0(), TTL)
            .unwrap();
        let admitted = store
            .redeem(Some("t-1"), t0() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(admitted.member, "user-17");
        assert_eq!(admitted.room, "room");
        assert_eq!(admitted.name.as_deref(), Some("Ada"));
        assert_eq!(
            admitted.metadata.get("role").map(String::as_str),
            Some("host")
        );
        assert!(admitted.grants.is_empty(), "no grants unless asked for");
    }

    #[test]
    fn a_ticket_is_valid_once() {
        let store = TicketStore::new();
        store
            .issue("t-1".into(), admission("room", "user-17"), t0(), TTL)
            .unwrap();
        assert!(store.redeem(Some("t-1"), t0()).is_ok());
        assert_eq!(store.redeem(Some("t-1"), t0()), Err(TicketError::Unknown));
    }

    #[test]
    fn a_ticket_expires() {
        let store = TicketStore::new();
        let expires = store
            .issue("t-1".into(), admission("room", "user-17"), t0(), TTL)
            .unwrap();
        assert_eq!(expires, t0() + TTL);
        assert_eq!(
            store.redeem(Some("t-1"), t0() + TTL),
            Err(TicketError::Expired)
        );
        // Refused for expiry, it is still consumed.
        assert_eq!(store.redeem(Some("t-1"), t0()), Err(TicketError::Unknown));
    }

    #[test]
    fn a_missing_or_unknown_ticket_is_refused() {
        let store = TicketStore::new();
        assert_eq!(store.redeem(None, t0()), Err(TicketError::Missing));
        assert_eq!(store.redeem(Some(""), t0()), Err(TicketError::Missing));
        assert_eq!(store.redeem(Some("nope"), t0()), Err(TicketError::Unknown));
    }

    #[test]
    fn two_members_of_one_room_have_their_own_tickets() {
        let store = TicketStore::new();
        store
            .issue("a".into(), admission("room", "ada"), t0(), TTL)
            .unwrap();
        store
            .issue("b".into(), admission("room", "bob"), t0(), TTL)
            .unwrap();
        assert_eq!(store.pending_for_room("room", t0()), 2);
        assert_eq!(store.redeem(Some("b"), t0()).unwrap().member, "bob");
        assert_eq!(store.redeem(Some("a"), t0()).unwrap().member, "ada");
        assert_eq!(store.pending_for_room("room", t0()), 0);
    }

    #[test]
    fn revoking_a_member_forgets_only_its_tickets() {
        let store = TicketStore::new();
        store
            .issue("a1".into(), admission("room", "ada"), t0(), TTL)
            .unwrap();
        store
            .issue("a2".into(), admission("room", "ada"), t0(), TTL)
            .unwrap();
        store
            .issue("a3".into(), admission("other", "ada"), t0(), TTL)
            .unwrap();
        store
            .issue("b".into(), admission("room", "bob"), t0(), TTL)
            .unwrap();
        assert_eq!(store.revoke_member("room", "ada"), 2);
        assert_eq!(store.redeem(Some("a1"), t0()), Err(TicketError::Unknown));
        assert!(store.redeem(Some("a3"), t0()).is_ok());
        assert!(store.redeem(Some("b"), t0()).is_ok());
    }

    #[test]
    fn revoking_a_room_forgets_all_its_tickets() {
        let store = TicketStore::new();
        store
            .issue("a".into(), admission("room", "ada"), t0(), TTL)
            .unwrap();
        store
            .issue("b".into(), admission("room", "bob"), t0(), TTL)
            .unwrap();
        store
            .issue("c".into(), admission("other", "cy"), t0(), TTL)
            .unwrap();
        assert_eq!(store.revoke_room("room"), 2);
        assert_eq!(store.pending_for_room("room", t0()), 0);
        assert_eq!(store.pending_for_room("other", t0()), 1);
    }

    #[test]
    fn expired_tickets_do_not_count_and_are_forgotten_on_the_next_issue() {
        let store = TicketStore::new();
        store
            .issue("old".into(), admission("room", "ada"), t0(), TTL)
            .unwrap();
        let later = t0() + TTL + Duration::from_secs(1);
        assert_eq!(store.pending_for_room("room", later), 0);
        store
            .issue("new".into(), admission("room", "bob"), later, TTL)
            .unwrap();
        assert_eq!(store.pending.lock().unwrap().len(), 1);
    }

    #[test]
    fn an_admission_must_name_a_member_a_room_and_known_grants() {
        let store = TicketStore::new();
        let invalid = |admission: Admission, ttl: Duration| {
            matches!(
                store.issue("t".into(), admission, t0(), ttl),
                Err(TicketError::Invalid(_))
            )
        };
        assert!(invalid(admission("room", ""), TTL));
        assert!(invalid(admission("", "ada"), TTL));
        assert!(invalid(admission("room", "ada"), Duration::ZERO));

        let mut with_grant = admission("room", "ada");
        with_grant.grants.push(TicketGrant {
            op: "delete".into(),
            node: "turn".into(),
            own: false,
            methods: Vec::new(),
        });
        assert!(invalid(with_grant.clone(), TTL));
        with_grant.grants[0].op = "tap".into();
        with_grant.grants[0].node = String::new();
        assert!(invalid(with_grant.clone(), TTL));
        with_grant.grants[0].node = "turn".into();
        assert!(store.issue("t".into(), with_grant, t0(), TTL).is_ok());
    }

    #[test]
    fn room_keys_do_not_collide() {
        assert_ne!(room_key("a:b", "c"), room_key("a", "b:c"));
        assert_ne!(room_key("1", "1:x"), room_key("1:1", "x"));
        assert_eq!(room_key("agent", "k"), room_key("agent", "k"));
    }
}
